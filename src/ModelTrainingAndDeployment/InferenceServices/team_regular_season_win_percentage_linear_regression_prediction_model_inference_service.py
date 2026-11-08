import json
import os
import pickle
import time
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.responses import Response

from src.ModelTrainingAndDeployment.feature_schema import FEATURE_COLUMNS, MODEL_NAME as DEFAULT_MODEL_NAME


MODEL_NAME = os.getenv("MODEL_NAME", DEFAULT_MODEL_NAME)

REQUEST_COUNT = Counter(
    "team_win_prediction_requests_total",
    "Total prediction requests",
)

ERROR_COUNT = Counter(
    "team_win_prediction_errors_total",
    "Total prediction errors",
)

REQUEST_LATENCY = Histogram(
    "team_win_prediction_request_latency_seconds",
    "Prediction request latency in seconds",
)

model = None


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


def check_feature_columns(registered_columns) -> None:
    """Refuse to serve a model that was trained on different features than this service sends."""
    if registered_columns is not None and list(registered_columns) != FEATURE_COLUMNS:
        raise RuntimeError(
            "Training/serving mismatch: the registered model was trained on "
            f"{list(registered_columns)} but this service sends {FEATURE_COLUMNS}"
        )


def load_model(model_uri: str | None):
    if not model_uri:
        raise RuntimeError("MODEL_URI environment variable is required")

    model_file_path = os.path.join(model_uri, "model.pkl")
    if not os.path.isfile(model_file_path):
        raise RuntimeError(
            f"No model.pkl in {model_uri}. This service loads from a LocalFileModelRegistry "
            "directory; loading from an MLflow models:/ URI is not implemented yet."
        )

    with open(model_file_path, "rb") as model_file:
        loaded_model = pickle.load(model_file)

    metadata_file_path = os.path.join(model_uri, "metadata.json")
    if os.path.isfile(metadata_file_path):
        with open(metadata_file_path, encoding="utf-8") as metadata_file:
            check_feature_columns(json.load(metadata_file).get("feature_columns"))

    return loaded_model


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global model
    model = load_model(os.getenv("MODEL_URI"))
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "model_name": MODEL_NAME,
        "model_loaded": model is not None,
    }


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/predict")
def predict(request: TeamRegularSeasonWinCountPredictionRequest):
    REQUEST_COUNT.inc()
    start_time = time.perf_counter()

    try:
        if model is None:
            raise HTTPException(status_code=500, detail="Model not loaded")

        # A DataFrame with named columns: scikit-learn checks the names and order against training.
        features = pd.DataFrame([request.model_dump()], columns=FEATURE_COLUMNS)
        prediction = float(model.predict(features)[0])

        return {
            "model_name": MODEL_NAME,
            "predicted_regular_season_wins": prediction,
        }
    except Exception as error:
        ERROR_COUNT.inc()
        raise HTTPException(status_code=500, detail=str(error))
    finally:
        REQUEST_LATENCY.observe(time.perf_counter() - start_time)
