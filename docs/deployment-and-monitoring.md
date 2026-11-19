# Deployment, evaluation and drift

## The life of a model version

1. **Train.** The trainer drops every row in the frozen holdout, fits the model, and registers it with:
   - the exact feature list
   - `training_data_sha256`, a fingerprint of the rows it was fitted on
   - `holdout_version` and `holdout_sha256`, proving which holdout it was kept away from
   - `holdout_metrics`, its accuracy on unseen data (the baseline for concept drift)
   - `drift_baseline`, quantile bins and proportions for every feature and for its own predictions
2. **Gate** (`deploy-model` workflow). The candidate and the current champion are scored on the same holdout. The candidate is rejected if:
   - its overall RMSE is worse than the champion's (default tolerance: 0%)
   - any segment (conference) got more than 15% worse, even if the average improved
   - it wasn't trained against this holdout, or its feature list doesn't match
3. **Roll out** with Argo Rollouts, using one of two strategies (below). The pods pin an exact version (`models:/<name>/<version>`), so every pod in a ReplicaSet serves the same model.
4. **Promote.** Only after the rollout is healthy does the `champion` alias move to the new version. The version it replaced is saved as `previous_champion`, so rollback is a single command.
5. **Monitor.** Service metrics are scraped continuously. The drift job runs daily against whatever `champion` points to.

## Canary vs blue-green

Both overlays share everything except the Rollout and its Services. Pick per deployment in the workflow.

| | Canary (`k8s/rollouts/canary`) | Blue-green (`k8s/rollouts/blue-green`) |
|---|---|---|
| How traffic moves | 10%, then analysis, then 50%, then analysis, then 100% | The new version gets no user traffic, is smoke-tested, then takes 100% at once |
| What gets checked | Error rate, p99 latency, and whether the new version's average prediction differs from the old one's on the same traffic | Smoke test before the switch; error rate and latency after |
| Failure | Analysis fails: Argo aborts, the canary scales to zero, stable keeps serving | Smoke test fails: no switch happens. Post-switch analysis fails: traffic flips back to blue, which is still running |
| Cost | Little extra capacity | Double capacity during the rollout, plus 10 minutes after |
| Use when | A bad model reaching everyone at once would be expensive, and you want real-traffic evidence first | You want an all-at-once cutover with an instant way back, and the model is already validated offline |

The canary's **prediction-shift** check is the model-specific one. Stable and canary pods see the same mix of requests, so their average predictions should be close. A big gap means the model behaves differently in production than in evaluation, which is what training-serving skew, a wrong model version, or a preprocessing bug looks like. The offline gate can't catch those.

## Data drift vs concept drift

**Data drift** asks whether the inputs still look like training. For each feature, live values are binned with the training baseline's quantile edges and compared with the Population Stability Index:

```
PSI = sum over bins of (live% - train%) * ln(live% / train%)
< 0.1 stable    0.1 - 0.25 warning    > 0.25 alert
```

It needs no labels, so it works from the first request. The same check on the model's outputs (prediction drift) is an early warning.

**Concept drift** asks whether, for the same kind of input, the right answer changed. It can only be measured once real outcomes arrive. Every prediction is logged with a `request_id`. Outcomes are written to `s3://<bucket>/labels/` as `{"request_id": ..., "actual": ...}`. The drift job joins them and compares live RMSE to the holdout RMSE recorded at training time. A ratio above 1.25 alerts.

The job reports both, plus what the combination usually means:

| Data drift | Concept drift | Usually means | Next step |
|---|---|---|---|
| no | no | healthy | nothing |
| yes | no | inputs moved, accuracy holding | find the cause (pipeline change? new population?), keep watching |
| no | yes | the relationship changed | retrain on recent labeled data, gate, canary |
| yes | yes | inputs moved and accuracy dropped | check the upstream data first (a broken feature job looks like drift), then retrain |

Things that look like drift but aren't, and are caught elsewhere: a broken feature pipeline (check upstream before retraining), a serving bug or skew (the canary prediction-shift check), and a new model version (the drift job only looks at predictions from the version currently serving).

## Security choices

- Pods run as non-root with a read-only root filesystem, and the namespace enforces the `restricted` Pod Security Standard.
- Each workload has its own IAM role, scoped to the S3 prefixes it needs (IRSA). The inference pods have no AWS role at all; they get models through the MLflow server.
- The artifacts bucket is KMS-encrypted, versioned, blocks public access, and denies non-TLS requests. Kubernetes Secrets are encrypted with a cluster KMS key.
- GitHub Actions uses OIDC roles, so no AWS keys are stored in the repository. Pull requests get a read-only plan role; only `main` and the `production` environment can deploy.
- Prediction logs contain the request features. For a system with personal or health data, they'd need the same protection as the source data, or de-identification before logging.

## Operating it

```bash
kubectl argo rollouts get rollout inference -n mlops --watch    # watch a rollout
kubectl argo rollouts abort inference -n mlops                  # stop a bad rollout
kubectl argo rollouts undo inference -n mlops                   # go back after it finished
python -m src.ModelTrainingAndDeployment.ModelRegistries.promote_model rollback
```

The **rollback-model** workflow does the last two together.
