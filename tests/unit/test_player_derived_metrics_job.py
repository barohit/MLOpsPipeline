from src.FeatureEngineering.Jobs.player_derived_metrics import PlayerDerivedMetricsJob


def test_player_derived_metrics_job(spark):
    players_rows = [
        (1, "Player A", 10, "G", "JR", 74, "Boston"),
    ]
    players_columns = [
        "id",
        "player_name",
        "team_id",
        "position",
        "class_year",
        "height_inches",
        "hometown",
    ]

    stats_rows = [
        (100, 1, 2026, 500, 100, 50, 80, 30, 40, 30, 60, 20, 80, 25, 10, 40, 5, 35),
    ]
    stats_columns = [
        "id",
        "player_id",
        "season_year",
        "minutes_played",
        "two_point_attempts",
        "two_point_made",
        "three_point_attempts",
        "three_point_made",
        "free_throw_attempts",
        "free_throw_made",
        "total_assists",
        "offensive_rebounds",
        "defensive_rebounds",
        "steals",
        "blocks",
        "total_defensive_fouls",
        "total_offensive_fouls",
        "total_turnovers",
    ]

    players_df = spark.createDataFrame(players_rows, players_columns)
    stats_df = spark.createDataFrame(stats_rows, stats_columns)

    job = PlayerDerivedMetricsJob()
    
    result = job.process_data({
        "players": players_df,
        "player_season_stats": stats_df,
    }).collect()[0].asDict()

    assert result["team_id"] == 10
    assert result["total_points"] == (50 * 2) + (30 * 3) + 30
    assert result["total_rebounds"] == 20 + 80

    assert round(result["two_point_percentage"], 4) == 0.5
    assert round(result["three_point_percentage"], 4) == round(30 / 80, 4)
    assert round(result["free_throw_percentage"], 4) == round(30 / 40, 4)

    assert round(result["assist_turnover_ratio"], 4) == round(60 / 35, 4)
    assert result["non_offensive_foul_turnovers"] == 35 - 5