from src.Infrastructure.Interfaces.base_feature_process import BaseFeatureProcess
from src.FeatureEngineering.Jobs.player_derived_metrics import PlayerDerivedMetricsJob


class PlayerDerivedMetricsFeatureProcess(BaseFeatureProcess):
    def __init__(self, spark, data_source, feature_store):
        super().__init__(
            spark=spark,
            data_source=data_source,
            job=PlayerDerivedMetricsJob(),
            feature_store=feature_store,
        )

    def run(self) -> None:
        super().run(
            source_tables=["players", "player_season_stats"],
            output_table="player_derived_metrics_features",
        )