# MLOps Pipeline

An end-to-end ML pipeline I built to get hands-on with the parts of the MLOps lifecycle I hadn't done professionally: ingesting data, computing features with PySpark, training, registering a model, serving it, and monitoring the service.

The use case is predicting an NCAA men's basketball team's regular-season wins. Teams and rosters are seeded by hand and the player stats are randomly generated, so the model has very little real signal. The model is a baseline. The pipeline around it is the point.

## How it fits together

```
Flask ingestion API  ->  Postgres source tables (teams, players, player_season_stats)
  -> PySpark feature jobs  ->  feature tables
  -> training dataset  ->  training (linear regression, MAE / RMSE / R²)
  -> model registry (local files or MLflow)
  -> Kubernetes manifests (Deployment, Service, Grafana dashboard)
  -> FastAPI inference service with Prometheus metrics
```

Everything that touches infrastructure sits behind a base class in `src/Infrastructure/Interfaces`: data source, feature store, credentials provider, model registry, model deployer. Postgres and Snowflake, local files and MLflow, and the three credential providers (environment, AWS Secrets Manager, Vault) are interchangeable without touching the jobs. There is also an in-memory data source and feature store, so the whole pipeline can run in a test with no database.

`src/ModelTrainingAndDeployment/feature_schema.py` lists the feature columns once. Training and the inference service both import it, and the service refuses to start if the registered model was trained on a different feature list.

## What is tested and what is not

Run automatically by `pytest`:

- The feature jobs (player metrics, roster composition, conference jobs, team win percentage).
- The whole pipeline in one process, with no database: source tables -> four feature processes -> training -> local registry -> inference service (`/predict`, `/metrics`). It also checks that the service refuses a model whose feature list doesn't match, and that every metric the Grafana dashboard queries is actually exported.
- The MLflow registry: registers a model, reloads it, and checks versions increment (SQLite backend).
- The Kubernetes deployer: checks the generated manifests (probes, env, scrape annotations, service port).
- The ingestion API, against a fake database connection.

Not tested yet:

- Postgres and Snowflake over JDBC. The code is written but has never run against a real database. The SQL scripts parse as valid PostgreSQL but are not loaded in an automated test.
- The AWS Secrets Manager and Vault credential providers.
- The Kubernetes manifests are generated and checked, but never applied to a cluster.
- The Docker image for the inference service has not been built yet.

Known gaps:

- The inference service loads a model from a local registry directory only. The deployer passes the registry URI to the pod as `MODEL_URI`, but the pod would need that directory mounted. Loading `models:/...` from MLflow in the service is not implemented.
- Monitoring is service-level only: request rate, errors, latency. There is no data drift or model-quality monitoring.
- Feature stores overwrite their tables each run, so there is no point-in-time history.
- Spark runs locally. There is no orchestration (Airflow / Step Functions), no EMR, no CI and no Terraform yet.

## Running it

```bash
pip install -r requirements.txt
python -m pytest
```

To run against a real database, create the tables and seed data with the scripts in `src/DataIngestion/sql/` (`setup_data_source_tables.sql` first), then start the ingestion API with `PGUSER` and `PGPASSWORD` set (`PGHOST`, `PGPORT` and `PGDATABASE` are optional).

MLflow: use a database backend, for example `MLflowModelRegistry(tracking_uri="sqlite:///mlflow.db", experiment_name="win-model")`. Current MLflow versions refuse the plain `./mlruns` file store by default.

The inference image is built from the repo root:

```bash
docker build -f src/ModelTrainingAndDeployment/InferenceServices/Dockerfile -t mlops-inference .
```

Training and serving must use the same scikit-learn version, because the model is pickled.

## Next

- Data drift monitoring on the inference service, exported to Prometheus
- Load models from MLflow in the inference service
- GitHub Actions to run the tests on every push
- Orchestration for the feature and training jobs, and Terraform for the AWS side
