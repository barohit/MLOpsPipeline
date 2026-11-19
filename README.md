# MLOps Pipeline

An end-to-end ML pipeline I built to get hands-on with the parts of the MLOps lifecycle I hadn't done professionally: ingesting data, computing features with PySpark, training against a frozen holdout, registering a model, gating it, deploying it to Kubernetes with canary or blue-green rollouts, and monitoring it for service health, data drift and concept drift.

The use case is predicting an NCAA men's basketball team's regular-season wins. Teams and rosters are seeded by hand and the player stats are randomly generated, so the model has very little real signal. The model is a baseline. The pipeline around it is the point.

## How it fits together

```
Flask ingestion API  ->  Postgres source tables (teams, players, player_season_stats)
  -> PySpark feature jobs  ->  feature tables  ->  training dataset
  -> frozen holdout (hash-split by team, checksummed, never trained on)
  -> training: excludes the holdout, records a training-data fingerprint and a drift baseline
  -> MLflow registry (versions + "champion" / "challenger" aliases)
  -> evaluation gate in CI: candidate vs champion on the same holdout, overall and per segment
  -> image to ECR  ->  Argo Rollout on EKS (canary or blue-green, with automated analysis)
  -> FastAPI inference service: logs every prediction with a request ID, exports Prometheus metrics
  -> daily drift CronJob: PSI per feature (data drift), live vs holdout RMSE (concept drift)
  -> Prometheus alerts + Grafana dashboard
```

Infrastructure is Terraform (`infra/terraform`), Kubernetes is kustomize (`k8s/`), CI/CD is GitHub Actions (`.github/workflows`). [docs/deployment-and-monitoring.md](docs/deployment-and-monitoring.md) explains the rollout strategies, the gate and the drift checks in more detail.

Everything that touches infrastructure sits behind a base class in `src/Infrastructure/Interfaces`: data source, feature store, credentials provider, model registry, model deployer. There is also an in-memory data source and feature store, so the whole pipeline can run in a test with no database.

`src/ModelTrainingAndDeployment/feature_schema.py` lists the feature columns once. Training and the inference service both import it, and the service refuses to start if the registered model was trained on a different feature list.

## Repository layout

| Path | What's there |
|---|---|
| `src/DataIngestion` | Flask ingestion API and SQL seed scripts |
| `src/FeatureEngineering` | PySpark feature jobs, feature processes, feature stores |
| `src/ModelTrainingAndDeployment` | training, `Evaluation/` (holdout + gate), registries + model loader, inference service |
| `src/MetricsAndMonitoring/Drift` | drift baseline, data drift (PSI), concept drift, prediction log, drift CLI |
| `infra/terraform` | state bootstrap; VPC, EKS, ECR, encrypted S3, IRSA roles, GitHub OIDC roles; Helm add-ons |
| `k8s/` | Argo Rollouts (canary, blue-green), analysis templates, PodMonitor, alert rules, drift CronJob, MLflow server |
| `.github/workflows` | CI, Terraform plan/apply, model deploy, model rollback |

## What is tested and what is not

Run automatically by `pytest` (51 tests):

- The feature jobs, and the whole pipeline in one process with no database: source tables -> feature processes -> training -> registry -> inference service.
- Holdout: deterministic assignment, checksum verification, immutability.
- Evaluation gate: passes an equal or better model, rejects a worse one, rejects one that got worse on a single segment even when the average improved, and rejects a model that wasn't trained against the current holdout.
- Data drift (PSI) and concept drift (live vs holdout RMSE), including an end-to-end run: serve predictions, log them, run the drift CLI on the logs, and check that shifted inputs and changed outcomes raise alerts while training-like traffic does not.
- MLflow: register, load by version and by alias, promote, roll back (SQLite backend).
- The inference service: request IDs, model version in responses and metrics, readiness vs liveness, prediction logging.

Checked but never run against real infrastructure:

- Terraform passes `fmt` and `validate` (checked with OpenTofu and AWS provider 5.80). It has never been applied.
- Both kustomize overlays build, and all 41 objects pass `kubeconform` against Kubernetes, Argo Rollouts and Prometheus Operator schemas. The alert rules and analysis queries pass `promtool`. Nothing has been applied to a cluster.
- The GitHub Actions workflows pass `actionlint`. They have never run.

Not tested:

- Postgres and Snowflake over JDBC; the AWS Secrets Manager and Vault credential providers.
- The Docker images have not been built.
- The Fluent Bit configuration that ships prediction logs to S3.

Known gaps:

- Feature tables are still overwritten each run. Models now record a fingerprint of their exact training rows, but the feature data itself isn't stored per version.
- The dev MLflow server uses SQLite on a volume; production would use RDS Postgres.
- Canary traffic is split by pod count. Exact percentages need a traffic router (AWS Load Balancer Controller or a service mesh).
- There's no orchestration for the feature and training jobs (Airflow / Step Functions) yet.

## Running it

```bash
pip install -r requirements.txt
python -m pytest
```

Freeze a holdout from an export of the training table, then gate a model against it:

```bash
python -m src.ModelTrainingAndDeployment.Evaluation.create_holdout \
  --input team_training_dataset_features.csv --output-dir holdouts --version v1

python -m src.ModelTrainingAndDeployment.Evaluation.run_gate \
  --holdout-dir holdouts/v1 --candidate-uri <model uri> --production-uri <model uri> --report gate.json
```

Run the inference service locally with prediction logging, then check the logs for drift:

```bash
MODEL_URI=<registry dir or models:/name/version> PREDICTION_LOG_PATH=predictions.jsonl \
  uvicorn src.ModelTrainingAndDeployment.InferenceServices.team_regular_season_win_percentage_linear_regression_prediction_model_inference_service:app

python -m src.MetricsAndMonitoring.Drift.run_drift_check \
  --baseline-file <registry dir>/metadata.json --predictions predictions.jsonl --report drift.json
```

To run against a real database, create the tables and seed data with the scripts in `src/DataIngestion/sql/` (`setup_data_source_tables.sql` first), then start the ingestion API with `PGUSER` and `PGPASSWORD` set.

MLflow: use a database backend, for example `MLflowModelRegistry(tracking_uri="sqlite:///mlflow.db", experiment_name="win-model")`. Current MLflow versions refuse the plain `./mlruns` file store by default.

Training and serving must use the same scikit-learn version, because the model is pickled.

## Deploying to AWS

1. `infra/terraform/bootstrap`: create the state bucket and lock table.
2. `infra/terraform/envs/dev`: copy `backend.hcl.example` and `terraform.tfvars.example`, fill them in, then `terraform init -backend-config=backend.hcl` and `terraform apply`.
3. Put the Terraform outputs into `k8s/` (the `REPLACE_WITH_...` values) and into GitHub repository variables: `AWS_REGION`, `AWS_PLAN_ROLE_ARN`, `AWS_DEPLOY_ROLE_ARN`, `EKS_CLUSTER_NAME`, `ECR_REPOSITORY_URL`, `ARTIFACTS_BUCKET`, `TF_STATE_BUCKET`, `TF_LOCK_TABLE`.
4. Build and push the MLflow image (`infra/mlflow`), then `kubectl apply -k k8s/rollouts/canary`.
5. Upload a frozen holdout to `s3://<bucket>/holdouts/v1/`, register a model, and run the **deploy-model** workflow with its version.

Run `terraform destroy` when you're done. EKS, the NAT gateway and the nodes cost money every hour they're up.
