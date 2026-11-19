"""A standardized, frozen holdout set.

Every candidate model is scored on the same rows, so "the new model is better" means
the same thing every time. Two properties make that work:

1. Assignment is deterministic. A row's side (holdout or training) comes from a hash
   of its key, not a random split, so the same team is always held out, re-running
   the pipeline never leaks holdout rows into training, and new keys get assigned
   consistently.
2. The holdout is frozen to disk with a manifest and a SHA-256 checksum. If anyone
   edits the file, loading it fails instead of silently changing the benchmark.
"""
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


HOLDOUT_FILE_NAME = "holdout.csv"
MANIFEST_FILE_NAME = "manifest.json"
HASH_BUCKETS = 10_000


class HoldoutIntegrityError(RuntimeError):
    """The frozen holdout on disk no longer matches its manifest."""


def is_holdout_key(key, fraction: float, salt: str) -> bool:
    if not 0 < fraction < 1:
        raise ValueError("fraction must be between 0 and 1")

    digest = hashlib.sha256(f"{salt}:{key}".encode("utf-8")).hexdigest()
    bucket = int(digest[:8], 16) % HASH_BUCKETS
    return bucket < fraction * HASH_BUCKETS


def split_holdout(
    df: pd.DataFrame,
    key_column: str,
    fraction: float = 0.2,
    salt: str = "holdout-v1",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (train_df, holdout_df), split by a hash of key_column."""
    if key_column not in df.columns:
        raise KeyError(f"{key_column} is not a column")

    mask = df[key_column].map(lambda key: is_holdout_key(key, fraction, salt))
    return df[~mask].copy(), df[mask].copy()


def _csv_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, float_format="%.10g", lineterminator="\n")
    return buffer.getvalue().encode("utf-8")


def freeze_holdout(
    holdout_df: pd.DataFrame,
    directory: str,
    version: str,
    key_column: str,
    feature_columns: list[str],
    target_column: str,
    salt: str,
    fraction: float,
    extra_columns: list[str] | None = None,
) -> dict:
    """Write the holdout and its manifest. Refuses to overwrite an existing version.

    extra_columns are kept for reporting only, e.g. conference_id for per-segment RMSE.
    """
    output_directory = Path(directory) / version
    if (output_directory / MANIFEST_FILE_NAME).exists():
        raise FileExistsError(
            f"Holdout {version} already exists. Holdouts are immutable; create a new version."
        )

    extra_columns = [column for column in (extra_columns or []) if column not in (key_column, target_column, *feature_columns)]
    columns = [key_column, *extra_columns, *feature_columns, target_column]
    missing = [column for column in columns if column not in holdout_df.columns]
    if missing:
        raise KeyError(f"Holdout is missing columns: {missing}")

    frozen = holdout_df[columns].sort_values(key_column).reset_index(drop=True)
    content = _csv_bytes(frozen)

    output_directory.mkdir(parents=True, exist_ok=True)
    (output_directory / HOLDOUT_FILE_NAME).write_bytes(content)

    manifest = {
        "version": version,
        "sha256": hashlib.sha256(content).hexdigest(),
        "row_count": int(len(frozen)),
        "key_column": key_column,
        "extra_columns": extra_columns,
        "feature_columns": list(feature_columns),
        "target_column": target_column,
        "salt": salt,
        "fraction": fraction,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (output_directory / MANIFEST_FILE_NAME).write_text(json.dumps(manifest, indent=4), encoding="utf-8")
    return manifest


def load_holdout(directory: str) -> tuple[pd.DataFrame, dict]:
    """Load a frozen holdout version directory and verify its checksum."""
    version_directory = Path(directory)
    manifest = json.loads((version_directory / MANIFEST_FILE_NAME).read_text(encoding="utf-8"))
    content = (version_directory / HOLDOUT_FILE_NAME).read_bytes()

    actual = hashlib.sha256(content).hexdigest()
    if actual != manifest["sha256"]:
        raise HoldoutIntegrityError(
            f"Holdout {manifest['version']} checksum mismatch: manifest {manifest['sha256']}, file {actual}"
        )

    df = pd.read_csv(io.BytesIO(content))
    if len(df) != manifest["row_count"]:
        raise HoldoutIntegrityError("Holdout row count does not match its manifest")

    return df, manifest
