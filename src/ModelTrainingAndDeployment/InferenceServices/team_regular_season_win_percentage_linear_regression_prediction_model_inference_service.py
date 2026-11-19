import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pandas as pd
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.responses import JSONResponse, Response

from src.Infrastructure.Interfaces.base_inference_service import BaseInferenceService
from src.MetricsAndMonitoring.Drift.prediction_log import PredictionLogger, build_prediction_logger
from src.ModelTrainingAndDeployment.ModelRegistries.model_loader import load_model
from src.ModelTrainingAndDeployment.feature_schema import FEATURE_COLUMNS, MODEL_NAME as DEFAULT_MODEL_NAME


MODEL_NAME = os.getenv("MODEL_NAME", DEFAULT_MODEL_NAME)

# Every metric carries model_version, so blue/green and canary pods can be compared
REQUEST_COUNT = Counter(
    "team_win_prediction_requests_total",
    "Total prediction requests",
    ["model_version"],
)

ERROR_COUNT = Counter(
    "team_win_prediction_errors_total",
    "Total prediction errors",
    ["model_version"],
)

REQUEST_LATENCY = Histogram(
    "team_win_prediction_request_latency_seconds",
    "Prediction request latency in seconds",
    ["model_version"],
)

# The distribution of outputs: a cheap, label-free early warning, and what the canary
# analysis compares between the old and new version
PREDICTION_VALUE = Histogram(
    "team_win_prediction_value",
    "Predicted regular-season wins",
    ["model_version"],
    buckets=(0, 5, 10, 15, 20, 25, 30, 35, 40),
)

PREDICTION_LOG_ERRORS = Counter(
    "team_win_prediction_log_errors_total",
    "Predictions that could not be written to the prediction log",
    ["model_version"],
)

MODEL_INFO = Gauge(
    "team_win_model_info",
    "The model version this pod is serving (value is always 1)",
    ["model_name", "model_version"],
)


class TeamRegularSeasonWinCountPredictionRequest(BaseModel):
    team_points_per_minute: float
    team_two_point_percentage: float
    team_three_point_percentage: float
    team_free_throw_percentage: float
    team_assist_turnover_ratio: float
    team_non_offensive_foul_turnovers: float
    team_total_rebounds: float
    team_offensive_rebound_ratio: float
    team_steals_per_minute: float
    team_blocks_per_minute: float
    team_three_point_attempt_rate: float
    player_count: float
    avg_height_inches: float
    upperclassman_ratio: float
    experience_score: float
    team_high_impact_player_count: float


# The request fields and the training features must be the same set. Fail at import, not at 2am.
if set(TeamRegularSeasonWinCountPredictionRequest.model_fields) != set(FEATURE_COLUMNS):
    raise RuntimeError("Request fields do not match feature_schema.FEATURE_COLUMNS")


class TeamWinCountInferenceService(BaseInferenceService):
    """Serves the linear regression that predicts a team's regular-season wins."""

    @property
    def input_columns(self) -> list[str]:
        return list(FEATURE_COLUMNS)

    @property
    def output_name(self) -> str:
        return "predicted_regular_season_wins"

    def load(self) -> None:
        if not self.model_uri:
            raise RuntimeError("MODEL_URI environment variable is required")

        # A LocalFileModelRegistry directory, models:/<name>/<version> or models:/<name>@<alias>
        try:
            loaded = load_model(self.model_uri)
        except FileNotFoundError as error:
            raise RuntimeError(str(error)) from error

        self.check_feature_columns(loaded.metadata.get("feature_columns"))

        self.metadata = loaded.metadata
        self.model_version = loaded.version
        self.model = loaded.model

    def check_feature_columns(self, registered_columns) -> None:
        """Refuse to serve a model that was trained on different features than this service sends."""
        if registered_columns is not None and list(registered_columns) != self.input_columns:
            raise RuntimeError(
                "Training/serving mismatch: the registered model was trained on "
                f"{list(registered_columns)} but this service sends {self.input_columns}"
            )

    def predict(self, features: dict[str, float]) -> dict[str, float]:
        # A DataFrame with named columns: scikit-learn checks the names and order against training.
        frame = pd.DataFrame([features], columns=self.input_columns)
        return {self.output_name: float(self.model.predict(frame)[0])}


inference_service: TeamWinCountInferenceService | None = None
prediction_logger: PredictionLogger = build_prediction_logger(None)


def _model_version() -> str:
    return inference_service.model_version if inference_service is not None else "unknown"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global inference_service, prediction_logger
    inference_service = TeamWinCountInferenceService(os.getenv("MODEL_URI"))
    inference_service.load()
    prediction_logger = build_prediction_logger()

    version = inference_service.model_version
    MODEL_INFO.labels(model_name=MODEL_NAME, model_version=version).set(1)
    # Create the zero-valued series up front so dashboards and alerts see them immediately
    for metric in (REQUEST_COUNT, ERROR_COUNT, PREDICTION_LOG_ERRORS):
        metric.labels(model_version=version)
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/live")
def live():
    """Liveness: the process is up. Never checks the model, so a slow load isn't killed."""
    return {"status": "alive"}


@app.get("/ready")
def ready():
    """Readiness: only send traffic once the model is in memory."""
    if inference_service is None or not inference_service.is_loaded:
        return JSONResponse(status_code=503, content={"status": "loading"})
    return {"status": "ready", "model_version": inference_service.model_version}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "model_name": MODEL_NAME,
        "model_version": _model_version(),
        "model_loaded": inference_service is not None and inference_service.is_loaded,
    }


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/predict")
def predict(
    request: TeamRegularSeasonWinCountPredictionRequest,
    x_request_id: str | None = Header(default=None),
):
    version = _model_version()
    REQUEST_COUNT.labels(model_version=version).inc()
    start_time = time.perf_counter()

    # The caller's request ID if it sent one, so outcomes can be joined back later
    request_id = x_request_id or str(uuid.uuid4())

    try:
        if inference_service is None or not inference_service.is_loaded:
            raise HTTPException(status_code=500, detail="Model not loaded")

        features = request.model_dump()
        result = inference_service.predict(features)
        prediction = result[inference_service.output_name]
        PREDICTION_VALUE.labels(model_version=version).observe(prediction)

        try:
            prediction_logger.log({
                "request_id": request_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "model_name": MODEL_NAME,
                "model_version": version,
                "features": features,
                "prediction": prediction,
            })
        except Exception:
            # Losing a log line must never fail the prediction, but it is counted
            PREDICTION_LOG_ERRORS.labels(model_version=version).inc()

        return {
            "model_name": MODEL_NAME,
            "model_version": version,
            "request_id": request_id,
            **result,
        }
    except Exception as error:
        ERROR_COUNT.labels(model_version=version).inc()
        raise HTTPException(status_code=500, detail=str(error))
    finally:
        REQUEST_LATENCY.labels(model_version=version).observe(time.perf_counter() - start_time)
