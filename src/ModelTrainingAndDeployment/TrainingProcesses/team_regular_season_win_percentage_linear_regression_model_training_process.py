import hashlib

import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from src.Infrastructure.Interfaces.base_model_training_process import BaseModelTrainingProcess
from src.MetricsAndMonitoring.Drift.baseline import build_baseline
from src.ModelTrainingAndDeployment.Evaluation.evaluation import evaluate
from src.ModelTrainingAndDeployment.feature_schema import (
    FEATURE_COLUMNS,
    FEATURE_TABLE_NAME,
    MODEL_NAME,
    TARGET_COLUMN,
)

KEY_COLUMN = "team_id"


def fingerprint(df: pd.DataFrame, sort_column: str | None) -> str:
    """SHA-256 of the exact rows a model was fitted on. Two models with the same
    fingerprint saw the same data; a different fingerprint means the data changed."""
    ordered = df.sort_values(sort_column) if sort_column and sort_column in df.columns else df
    content = ordered.to_csv(index=False, float_format="%.10g", lineterminator="\n").encode("utf-8")
    return hashlib.sha256(content).hexdigest()


class TeamRegularSeasonWinCountLinearRegressionModelTrainer(BaseModelTrainingProcess):
    def __init__(
        self,
        spark,
        data_source,
        model_registry,
        holdout_df: pd.DataFrame | None = None,
        holdout_manifest: dict | None = None,
        baseline_bins: int = 10,
    ):
        super().__init__(data_source=data_source, model_registry=model_registry)
        self.spark = spark
        self.feature_table_name = FEATURE_TABLE_NAME
        self.model_name = MODEL_NAME
        self.feature_columns = list(FEATURE_COLUMNS)
        self.target_column = TARGET_COLUMN
        self.holdout_df = holdout_df
        self.holdout_manifest = holdout_manifest
        self.baseline_bins = baseline_bins

    def run(self):
        training_df = self.data_source.read_table(self.spark, self.feature_table_name)

        selected = [*self.feature_columns, self.target_column]
        if KEY_COLUMN in training_df.columns:
            selected = [KEY_COLUMN, *selected]

        pandas_df = training_df.select(*selected).toPandas().dropna()

        # The frozen holdout is never trained on, so the evaluation gate stays honest
        excluded_rows = 0
        if self.holdout_df is not None:
            key = (self.holdout_manifest or {}).get("key_column", KEY_COLUMN)
            holdout_keys = set(self.holdout_df[key])
            before = len(pandas_df)
            pandas_df = pandas_df[~pandas_df[key].isin(holdout_keys)]
            excluded_rows = before - len(pandas_df)

        X = pandas_df[self.feature_columns]
        y = pandas_df[self.target_column]

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.2,
            random_state=42
        )

        model = LinearRegression()
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        metrics = {
            "mae": float(mean_absolute_error(y_test, y_pred)),
            "rmse": float(mean_squared_error(y_test, y_pred) ** 0.5),
            "r2": float(r2_score(y_test, y_pred))
        }

        metadata = {
            "model_type": "linear_regression",
            "feature_table_name": self.feature_table_name,
            "feature_columns": self.feature_columns,
            "target_column": self.target_column,
            "train_row_count": int(len(X_train)),
            "test_row_count": int(len(X_test)),
            "coefficients": {
                feature_name: float(coefficient)
                for feature_name, coefficient in zip(self.feature_columns, model.coef_)
            },
            "intercept": float(model.intercept_),
            # Data versioning: exactly which rows this model learned from
            "training_data_sha256": fingerprint(pandas_df, KEY_COLUMN),
            "excluded_holdout_rows": int(excluded_rows),
            # What live traffic is compared against for data drift
            "drift_baseline": build_baseline(
                X_train, self.feature_columns, model.predict(X_train), bins=self.baseline_bins
            ),
        }

        if self.holdout_df is not None and self.holdout_manifest is not None:
            metadata["holdout_version"] = self.holdout_manifest["version"]
            metadata["holdout_sha256"] = self.holdout_manifest["sha256"]
            # The concept-drift baseline: how accurate the model was on unseen data
            metadata["holdout_metrics"] = evaluate(
                model, self.holdout_df, self.feature_columns, self.target_column
            )

        model_uri = self.model_registry.register_model(
            model=model,
            model_name=self.model_name,
            metadata={**metadata, "metrics": metrics}
        )

        return {
            "model_uri": model_uri,
            "metrics": metrics,
            "metadata": metadata
        }
