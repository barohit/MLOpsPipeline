from src.FeatureEngineering.Jobs.team_roster_composition_job import TeamRosterCompositionJob


def test_team_roster_composition_job(spark):
    rows = [
        (1, "Player A", 10, "G", "FR", 72, "Boston"),
        (2, "Player B", 10, "F", "JR", 78, "Miami"),
        (3, "Player C", 10, "C", "SR", 82, "Chicago"),
        (4, "Player D", 20, "G", "SO", 74, "Dallas"),
        (5, "Player E", 20, "G", "GR", 73, "Seattle"),
    ]

    columns = [
        "id",
        "player_name",
        "team_id",
        "position",
        "class_year",
        "height_inches",
        "hometown",
    ]

    players_df = spark.createDataFrame(rows, columns)

    job = TeamRosterCompositionJob()
    result_df = job.process_data({"players": players_df})

    results = {row["team_id"]: row.asDict() for row in result_df.collect()}

    team_10 = results[10]
    assert team_10["player_count"] == 3
    assert round(team_10["avg_height_inches"], 2) == round((72 + 78 + 82) / 3, 2)
    assert team_10["guard_count"] == 1
    assert team_10["forward_count"] == 1
    assert team_10["center_count"] == 1
    assert team_10["freshman_count"] == 1
    assert team_10["junior_count"] == 1
    assert team_10["senior_count"] == 1
    assert round(team_10["upperclassman_ratio"], 4) == round(2 / 3, 4)

    team_20 = results[20]
    assert team_20["player_count"] == 2
    assert team_20["guard_count"] == 2
    assert team_20["grad_count"] == 1
    assert round(team_20["guard_ratio"], 4) == 1.0