from FeatureEngineering.base.base_feature_process import BaseFeatureProcess
from FeatureEngineering.jobs.team_roster_composition_job import TeamRosterCompositionJob


class TeamRosterCompositionFeatureProcess(BaseFeatureProcess):
    def __init__(self, spark, data_source, feature_store):
        super().__init__(
            spark=spark,
            data_source=data_source,
            job=TeamRosterCompositionJob(),
            feature_store=feature_store,
        )

    def run(self) -> None:
        super().run(
            source_tables=["players"],
            output_table="team_roster_composition_features",
        )