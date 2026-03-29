import os
import pickle
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.responses import Response


MODEL_URI = os.getenv("MODEL_URI")
MODEL_NAME = os.getenv("MODEL_NAME", "team_regular_season_win_count_linear_regression")

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

app = FastAPI()
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


@app.on_event("startup")
def startup_event():
    global model

    if MODEL_URI is None:
        raise RuntimeError("MODEL_URI environment variable is required")

    model_file_path = os.path.join(MODEL_URI, "model.pkl")

    with open(model_file_path, "rb") as model_file:
        model = pickle.load(model_file)


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
    global model

    REQUEST_COUNT.inc()
    start_time = time.perf_counter()

    try:
        if model is None:
            raise HTTPException(status_code=500, detail="Model not loaded")

        feature_vector = [[
            request.team_points_per_minute,
            request.team_two_point_percentage,
            request.team_three_point_percentage,
            request.team_free_throw_percentage,
            request.team_assist_turnover_ratio,
            request.team_non_offensive_foul_turnovers,
            request.team_total_rebounds,
            request.team_offensive_rebound_ratio,
            request.team_steals_per_minute,
            request.team_blocks_per_minute,
            request.team_three_point_attempt_rate,
            request.player_count,
            request.avg_height_inches,
            request.upperclassman_ratio,
            request.experience_score,
            request.team_high_impact_player_count,
        ]]

        prediction = float(model.predict(feature_vector)[0])

        return {
            "model_name": MODEL_NAME,
            "predicted_regular_season_wins": prediction,
        }
    except Exception as error:
        ERROR_COUNT.inc()
        raise HTTPException(status_code=500, detail=str(error))
    finally:
        REQUEST_LATENCY.observe(time.perf_counter() - start_time)