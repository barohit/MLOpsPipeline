# MLflow tracking server for the dev cluster. The official image doesn't include
# boto3, which the server needs to proxy artifacts to and from S3.
#   docker build -t <ecr_mlflow_repository_url>:<tag> infra/mlflow
# Pin MLFLOW_VERSION to the release you test with.
ARG MLFLOW_VERSION=v3.1.0
FROM ghcr.io/mlflow/mlflow:${MLFLOW_VERSION}

RUN pip install --no-cache-dir boto3

RUN useradd --create-home --uid 10001 mlflow
USER 10001
