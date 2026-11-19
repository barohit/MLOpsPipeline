"""Freeze a new holdout version from an export of the training dataset table.

    python -m src.ModelTrainingAndDeployment.Evaluation.create_holdout \
        --input team_training_dataset_features.csv --output-dir holdouts --version v1

Run this once per holdout version. Every model trained afterwards excludes these
keys and is gated on these rows, until someone deliberately creates v2.
"""
import argparse
import sys

import pandas as pd

from src.ModelTrainingAndDeployment.Evaluation.holdout import freeze_holdout, split_holdout
from src.ModelTrainingAndDeployment.feature_schema import FEATURE_COLUMNS, TARGET_COLUMN

DEFAULT_KEY_COLUMN = "team_id"
DEFAULT_SEGMENT_COLUMN = "conference_id"


def create_holdout(
    training_df: pd.DataFrame,
    output_dir: str,
    version: str,
    fraction: float = 0.2,
    salt: str | None = None,
    key_column: str = DEFAULT_KEY_COLUMN,
    segment_column: str | None = DEFAULT_SEGMENT_COLUMN,
) -> dict:
    salt = salt or f"holdout-{version}"
    _, holdout_df = split_holdout(training_df.dropna(subset=[*FEATURE_COLUMNS, TARGET_COLUMN]),
                                  key_column=key_column, fraction=fraction, salt=salt)

    if holdout_df.empty:
        raise ValueError("The holdout came out empty; use a larger fraction or more data")

    extra = [segment_column] if segment_column and segment_column in training_df.columns else []
    return freeze_holdout(
        holdout_df,
        directory=output_dir,
        version=version,
        key_column=key_column,
        feature_columns=list(FEATURE_COLUMNS),
        target_column=TARGET_COLUMN,
        salt=salt,
        fraction=fraction,
        extra_columns=extra,
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Freeze a holdout version")
    parser.add_argument("--input", required=True, help="CSV export of the training dataset table")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--fraction", type=float, default=0.2)
    parser.add_argument("--salt")
    args = parser.parse_args(argv)

    manifest = create_holdout(
        pd.read_csv(args.input), args.output_dir, args.version, args.fraction, args.salt
    )
    print(f"Froze holdout {manifest['version']}: {manifest['row_count']} rows, sha256 {manifest['sha256']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
