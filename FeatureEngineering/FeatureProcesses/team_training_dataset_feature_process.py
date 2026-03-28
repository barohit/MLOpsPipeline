from FeatureEngineering.base.base_feature_process import BaseFeatureProcess
from FeatureEngineering.jobs.team_training_dataset_job import TeamTrainingDatasetJob


class TeamTrainingDatasetFeatureProcess(BaseFeatureProcess):
    def __init__(self, spark, data_source, feature_store):
        super().__init__(
            spark=spark,
            data_source=data_source,
            job=TeamTrainingDatasetJob(),
            feature_store=feature_store,
        )

    def run(self) -> None:
        super().run(
            source_tables=[
                "team_aggregated_stats_features",
                "team_roster_composition_features",
                "teams",
            ],
            output_table="team_training_dataset_features",
        )