# Build from the repo root:
#   docker build -f src/ModelTrainingAndDeployment/InferenceServices/Dockerfile -t mlops-inference .
FROM python:3.12-slim

WORKDIR /app

COPY src/ModelTrainingAndDeployment/InferenceServices/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY src/__init__.py src/__init__.py
COPY src/ModelTrainingAndDeployment src/ModelTrainingAndDeployment

EXPOSE 8000

CMD ["uvicorn", "src.ModelTrainingAndDeployment.InferenceServices.team_regular_season_win_percentage_linear_regression_prediction_model_inference_service:app", "--host", "0.0.0.0", "--port", "8000"]
