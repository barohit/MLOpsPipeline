import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from ModelTraining.base.base_model_training_process import BaseModelTrainingProcess


class TeamRegularSeasonWinCountLinearRegressionModelTrainer(BaseModelTrainingProcess):
    def __init__(self, spark, data_source, model_registry):
        super().__init__(data_source=data_source, model_registry=model_registry)
        self.spark = spark
        self.feature_table_name = "team_training_dataset_features"
        self.model_name = "team_regular_season_win_count_linear_regression"

        self.feature_columns = [
            "team_points_per_minute",
            "team_two_point_percentage",
            "team_three_point_percentage",
            "team_free_throw_percentage",
            "team_assist_turnover_ratio",
            "team_non_offensive_foul_turnovers",
            "team_total_rebounds",
            "team_offensive_rebound_ratio",
            "team_steals_per_minute",
            "team_blocks_per_minute",
            "team_three_point_attempt_rate",
            "player_count",
            "avg_height_inches",
            "upperclassman_ratio",
            "experience_score",
            "team_high_impact_player_count"
        ]

        self.target_column = "target_regular_season_wins"

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