import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from src.Infrastructure.Interfaces.base_model_training_process import BaseModelTrainingProcess
from src.ModelTrainingAndDeployment.feature_schema import (
    FEATURE_COLUMNS,
    FEATURE_TABLE_NAME,
    MODEL_NAME,
    TARGET_COLUMN,
)


class TeamRegularSeasonWinCountLinearRegressionModelTrainer(BaseModelTrainingProcess):
    def __init__(self, spark, data_source, model_registry):
        super().__init__(data_source=data_source, model_registry=model_registry)
        self.spark = spark
        self.feature_table_name = FEATURE_TABLE_NAME
        self.model_name = MODEL_NAME
        self.feature_columns = list(FEATURE_COLUMNS)
        self.target_column = TARGET_COLUMN

    def run(self):
        training_df = self.data_source.read_table(self.spark, self.feature_table_name)

        pandas_df = training_df.select(
            *self.feature_columns,
            self.target_column
        ).toPandas()

        pandas_df = pandas_df.dropna()

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
            "intercept": float(model.intercept_)
        }

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
