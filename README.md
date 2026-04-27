# MLOps Pipeline

This is an end-to-end MLOps pipeline — data ingestion, feature engineering, model training, deployment, and monitoring — built around swappable infrastructure components so the platform isn't tied to a specific vendor or backend.

The reference use case is predicting NCAA men's basketball regular season win percentage, but the pipeline itself is model-agnostic. The basketball stuff is just one example of what runs on top of it.

## What's in here

**Data ingestion** currently has SQL seed scripts for the source data (teams, players, conferences, season stats) plus a starter ingestion service. An actual REST API for ingestion is on the to-do list.

**Feature engineering** pulls data from the configured source (PostgreSQL or Snowflake), runs jobs to compute features, and publishes to a feature store. The feature store tables are defined in `src/DataIngestion/sql/setup_feature_store_tables.sql`. Feature processes combine the data extraction and the job logic into one object, and the design is set up so jobs can run locally or on external infra like EMR — the EMR/Spark integration itself isn't wired up yet, that's planned.

**Model training and deployment** is wired up end-to-end for the basketball use case: a linear regression training process, model registry (MLflow or local file), Kubernetes deployer, and a FastAPI inference service with a Dockerfile.

**Monitoring** uses Prometheus and Grafana — there's a monitoring service for tracking the win percentage prediction model in production.

**Infrastructure** is interface-driven. Data sources, feature stores, credentials providers, model registries, and deployers all sit behind base interfaces, so swapping PostgreSQL for Snowflake or local file storage for MLflow doesn't require touching the code that uses them. Credentials can come from environment variables, AWS Secrets Manager, or HashiCorp Vault depending on where you're running.

## Tech stack

Python, FastAPI, scikit-learn, PostgreSQL, Snowflake, MLflow, Kubernetes, Docker, Prometheus, Grafana, AWS Secrets Manager, HashiCorp Vault, Pydantic, pytest.

## Running it

```bash
pip install -r requirements.txt
```

Set up the database with the SQL scripts in `src/DataIngestion/sql/`, run a feature job from `src/FeatureEngineering/Jobs/`, train a model, then build and run the inference service in `src/ModelTrainingAndDeployment/InferenceServices/`.

## To-do

- REST API for data ingestion (currently SQL-only)
- Wire up Spark/EMR for distributed feature jobs
- Expand integration test coverage
- Terraform for AWS deployment
