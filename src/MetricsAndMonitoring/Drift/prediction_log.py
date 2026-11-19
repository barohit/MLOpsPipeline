"""Prediction logging, the raw material for both drift checks.

Each prediction is written as one JSON line with a request_id, so the true outcome
can be joined to it later (concept drift) and its inputs compared to the training
distribution (data drift).

In Kubernetes the service logs to stdout (PREDICTION_LOG_PATH=-); Fluent Bit picks up
lines with "log_type": "prediction" and ships them to S3, where the drift CronJob
reads them. Locally, point PREDICTION_LOG_PATH at a file.

This project's features are basketball stats. In a system with personal data, these
records would be PII/PHI and would need the same encryption and access controls as
the source data, or the features would need to be de-identified before logging.
"""
import json
import os
import sys
import threading
from abc import ABC, abstractmethod
from pathlib import Path

PREDICTION_LOG_TYPE = "prediction"


class PredictionLogger(ABC):
    @abstractmethod
    def log(self, record: dict) -> None:
        pass


class NullPredictionLogger(PredictionLogger):
    def log(self, record: dict) -> None:
        return None


class JsonlPredictionLogger(PredictionLogger):
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()

    def log(self, record: dict) -> None:
        line = json.dumps({"log_type": PREDICTION_LOG_TYPE, **record}, separators=(",", ":"))

        with self._lock:
            if self.path == "-":
                sys.stdout.write(line + "\n")
                sys.stdout.flush()
                return

            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as log_file:
                log_file.write(line + "\n")


def build_prediction_logger(path: str | None = None) -> PredictionLogger:
    path = path if path is not None else os.getenv("PREDICTION_LOG_PATH")
    return JsonlPredictionLogger(path) if path else NullPredictionLogger()


def _parse_lines(text: str) -> list[dict]:
    records = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def _read_s3(uri: str) -> list[dict]:
    import boto3

    bucket, _, prefix = uri[len("s3://"):].partition("/")
    s3 = boto3.client("s3")
    records = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            body = s3.get_object(Bucket=bucket, Key=item["Key"])["Body"].read()
            records.extend(_parse_lines(body.decode("utf-8")))
    return records


def read_jsonl(uri: str) -> list[dict]:
    """Read records from a file, a directory of *.jsonl files, or an s3://bucket/prefix."""
    if uri.startswith("s3://"):
        return _read_s3(uri)

    path = Path(uri)
    files = sorted(path.glob("**/*.jsonl")) if path.is_dir() else [path]
    records = []
    for file_path in files:
        records.extend(_parse_lines(file_path.read_text(encoding="utf-8")))
    return records


def read_predictions(uri: str, model_version: str | None = None, since: str | None = None) -> list[dict]:
    """Prediction records only, optionally for one model version and after an ISO timestamp."""
    records = [
        record for record in read_jsonl(uri)
        if record.get("log_type", PREDICTION_LOG_TYPE) == PREDICTION_LOG_TYPE and "prediction" in record
    ]
    if model_version is not None:
        records = [record for record in records if str(record.get("model_version")) == str(model_version)]
    if since is not None:
        records = [record for record in records if record.get("timestamp", "") >= since]
    return records
