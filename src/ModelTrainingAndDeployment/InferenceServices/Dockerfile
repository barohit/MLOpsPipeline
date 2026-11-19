# Build from the repo root:
#   docker build -f src/ModelTrainingAndDeployment/InferenceServices/Dockerfile -t mlops-inference .
#
# One image, two uses: the FastAPI inference service (default CMD) and the drift
# check CronJob (python -m src.MetricsAndMonitoring.Drift.run_drift_check ...).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY src/ModelTrainingAndDeployment/InferenceServices/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/__init__.py src/__init__.py
COPY src/Infrastructure src/Infrastructure
COPY src/MetricsAndMonitoring src/MetricsAndMonitoring
COPY src/ModelTrainingAndDeployment src/ModelTrainingAndDeployment

# Don't run as root inside the cluster
RUN useradd --create-home --uid 10001 appuser
USER 10001

EXPOSE 8000

CMD ["uvicorn", "src.ModelTrainingAndDeployment.InferenceServices.team_regular_season_win_percentage_linear_regression_prediction_model_inference_service:app", "--host", "0.0.0.0", "--port", "8000"]
