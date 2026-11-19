"""Scheduled drift check (runs as a Kubernetes CronJob).

    python -m src.MetricsAndMonitoring.Drift.run_drift_check \
        --model-uri models:/team_regular_season_win_count_linear_regression@champion \
        --predictions s3://<bucket>/prediction-logs/ \
        --labels s3://<bucket>/labels/ \
        --since-hours 168 \
        --pushgateway prometheus-pushgateway.monitoring:9091 \
        --report /tmp/drift_report.json

It writes a JSON report and, if a Pushgateway is given, pushes gauges that
Prometheus alert rules watch (see k8s/monitoring/prometheus-rules.yaml).
"""
import argparse
import json
import sys
from datetime import datetime, timedelta, timezone

from src.MetricsAndMonitoring.Drift.concept_drift import ConceptDriftDetector, diagnose
from src.MetricsAndMonitoring.Drift.data_drift import ALERT, INSUFFICIENT, OK, WARNING, DataDriftDetector
from src.MetricsAndMonitoring.Drift.prediction_log import read_jsonl, read_predictions

STATUS_VALUE = {INSUFFICIENT: -1, OK: 0, WARNING: 1, ALERT: 2}


def baseline_rmse_from_metadata(metadata: dict) -> float | None:
    for key in ("holdout_metrics", "metrics"):
        rmse = (metadata.get(key) or {}).get("rmse")
        if rmse:
            return float(rmse)
    return None


def build_report(metadata: dict, predictions: list[dict], labels: list[dict], args) -> dict:
    baseline = metadata.get("drift_baseline")
    if not baseline:
        raise ValueError("Model metadata has no drift_baseline; retrain with the current trainer")

    live_features = {column: [] for column in baseline["features"]}
    for record in predictions:
        for column, value in (record.get("features") or {}).items():
            if column in live_features and value is not None:
                live_features[column].append(float(value))

    data_report = DataDriftDetector(
        baseline,
        psi_warning=args.psi_warning,
        psi_alert=args.psi_alert,
        min_samples=args.min_samples,
    ).check(live_features, [record["prediction"] for record in predictions])

    baseline_rmse = baseline_rmse_from_metadata(metadata)
    if baseline_rmse is None or not labels:
        concept_report = {"status": INSUFFICIENT, "labeled_count": 0, "baseline_rmse": baseline_rmse}
    else:
        concept_report = ConceptDriftDetector(
            baseline_rmse,
            min_labeled=args.min_labeled,
            window_size=args.window_size,
        ).check(predictions, labels)

    return {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "model_name": metadata.get("model_name"),
        "model_version": str(metadata.get("version", args.model_version or "unknown")),
        "prediction_count": len(predictions),
        "data_drift": data_report,
        "concept_drift": concept_report,
        "diagnosis": diagnose(data_report["status"], concept_report["status"]),
    }


def push_metrics(report: dict, gateway: str, job: str) -> None:
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

    registry = CollectorRegistry()
    psi = Gauge("model_feature_psi", "PSI of a feature vs the training baseline", ["feature"], registry=registry)
    for feature, result in report["data_drift"]["features"].items():
        if result.get("psi") is not None:
            psi.labels(feature=feature).set(result["psi"])

    prediction = report["data_drift"].get("prediction")
    if prediction:
        Gauge("model_prediction_psi", "PSI of predictions vs training", registry=registry).set(prediction["psi"])

    status = Gauge("model_drift_status", "-1 insufficient, 0 ok, 1 warning, 2 alert", ["kind"], registry=registry)
    status.labels(kind="data").set(STATUS_VALUE[report["data_drift"]["status"]])
    status.labels(kind="concept").set(STATUS_VALUE[report["concept_drift"]["status"]])

    concept = report["concept_drift"]
    if concept.get("live_rmse") is not None:
        Gauge("model_live_rmse", "RMSE on recently labeled predictions", registry=registry).set(concept["live_rmse"])
        Gauge("model_live_rmse_ratio", "Live RMSE / holdout RMSE", registry=registry).set(concept["rmse_ratio"])

    Gauge("model_labeled_count", "Predictions with a known outcome", registry=registry).set(concept.get("labeled_count", 0))
    Gauge("model_live_prediction_count", "Predictions in the checked window", registry=registry).set(report["prediction_count"])
    Gauge("model_drift_check_last_run_timestamp_seconds", "When the drift check last ran", registry=registry).set_to_current_time()

    push_to_gateway(gateway, job=job, registry=registry, grouping_key={"model_version": report["model_version"]})


def load_metadata(args) -> dict:
    if args.baseline_file:
        with open(args.baseline_file, encoding="utf-8") as metadata_file:
            return json.load(metadata_file)

    from src.ModelTrainingAndDeployment.ModelRegistries.model_loader import load_model

    loaded = load_model(args.model_uri)
    return {**loaded.metadata, "version": loaded.version}


def run(args) -> dict:
    metadata = load_metadata(args)
    model_version = args.model_version or str(metadata.get("version", "")) or None

    since = None
    if args.since_hours:
        since = (datetime.now(timezone.utc) - timedelta(hours=args.since_hours)).isoformat()

    # Only judge the model that is actually serving: mixing versions hides drift
    predictions = read_predictions(args.predictions, model_version=model_version, since=since)
    labels = read_jsonl(args.labels) if args.labels else []

    report = build_report(metadata, predictions, labels, args)

    if args.report:
        with open(args.report, "w", encoding="utf-8") as report_file:
            json.dump(report, report_file, indent=4)

    if args.pushgateway:
        push_metrics(report, args.pushgateway, args.job_name)

    print(json.dumps({
        "data_drift": report["data_drift"]["status"],
        "concept_drift": report["concept_drift"]["status"],
        "diagnosis": report["diagnosis"],
    }, indent=2))
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check a served model for data and concept drift")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--model-uri", help="Registry URI; the drift baseline is read from its metadata")
    source.add_argument("--baseline-file", help="A metadata.json with drift_baseline, for local runs")
    parser.add_argument("--model-version", help="Only check predictions from this version")
    parser.add_argument("--predictions", required=True, help="File, directory or s3:// prefix of prediction logs")
    parser.add_argument("--labels", help="File, directory or s3:// prefix of {request_id, actual} records")
    parser.add_argument("--since-hours", type=float)
    parser.add_argument("--psi-warning", type=float, default=0.1)
    parser.add_argument("--psi-alert", type=float, default=0.25)
    parser.add_argument("--min-samples", type=int, default=50)
    parser.add_argument("--min-labeled", type=int, default=30)
    parser.add_argument("--window-size", type=int)
    parser.add_argument("--pushgateway")
    parser.add_argument("--job-name", default="model-drift-check")
    parser.add_argument("--report")
    return parser


def main(argv=None) -> int:
    try:
        run(build_parser().parse_args(argv))
        return 0
    except Exception as error:
        print(f"Drift check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
