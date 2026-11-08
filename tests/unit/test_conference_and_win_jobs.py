from src.FeatureEngineering.Jobs.conference_team_win_count_job import ConferenceTeamCountJob
from src.FeatureEngineering.Jobs.conference_win_rate_job import ConferenceWinRateJob
from src.FeatureEngineering.Jobs.team_win_percentage_job import TeamWinPercentageJob

TEAM_COLUMNS = ["id", "team_name", "conference_id", "regular_season_wins", "regular_season_losses"]
TEAM_ROWS = [
    (1, "A", 10, 30, 6),
    (2, "B", 10, 20, 10),
    (3, "C", 20, 15, 15),
]


def test_conference_team_count(spark):
    teams = spark.createDataFrame(TEAM_ROWS, TEAM_COLUMNS)
    result = {r["conference_id"]: r["team_count"] for r in ConferenceTeamCountJob().process_data({"teams": teams}).collect()}
    assert result == {10: 2, 20: 1}


def test_conference_win_rate(spark):
    teams = spark.createDataFrame(TEAM_ROWS, TEAM_COLUMNS)
    rows = {r["conference_id"]: r for r in ConferenceWinRateJob().process_data({"teams": teams}).collect()}
    assert rows[10]["total_wins"] == 50 and rows[10]["total_losses"] == 16
    assert round(rows[10]["conference_win_percentage"], 4) == round(50 / 66, 4)


def test_team_win_percentage_reads_the_teams_table(spark):
    teams = spark.createDataFrame(TEAM_ROWS, TEAM_COLUMNS)
    rows = {r["team_id"]: r for r in TeamWinPercentageJob().process_data({"teams": teams}).collect()}
    assert round(rows[1]["win_percentage"], 4) == round(30 / 36, 4)
    assert rows[3]["win_percentage"] == 0.5
