from FeatureEngineering.base.base_feature_process import BaseFeatureProcess
from FeatureEngineering.jobs.team_aggregated_stats_job import TeamAggregatedStatsJob


class TeamAggregatedStatsFeatureProcess(BaseFeatureProcess):
    def __init__(self, spark, data_source, feature_store):
        super().__init__(
            spark=spark,
            data_source=data_source,
            job=TeamAggregatedStatsJob(),
            feature_store=feature_store,
        )

    def run(self) -> None:
        super().run(
            source_tables=["player_derived_metrics_features"],
            output_table="team_aggregated_stats_features",
        )