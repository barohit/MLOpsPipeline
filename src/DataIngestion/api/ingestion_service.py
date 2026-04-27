from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor


app = Flask(__name__)


def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="mlops_pipeline",
        user="rohit",
        password="your_password_here",
        cursor_factory=RealDictCursor,
    )


@app.post("/teams")
def upsert_team():
    data = request.json

    query = """
    INSERT INTO teams (
        team_name,
        conference_id,
        regular_season_record,
        regular_season_wins,
        regular_season_losses
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (team_name)
    DO UPDATE SET
        conference_id = EXCLUDED.conference_id,
        regular_season_record = EXCLUDED.regular_season_record,
        regular_season_wins = EXCLUDED.regular_season_wins,
        regular_season_losses = EXCLUDED.regular_season_losses
    RETURNING *;
    """

    values = (
        data["team_name"],
        data["conference_id"],
        data["regular_season_record"],
        data["regular_season_wins"],
        data["regular_season_losses"],
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, values)
            result = cur.fetchone()

    return jsonify(result)


@app.post("/players")
def upsert_player():
    data = request.json

    query = """
    INSERT INTO players (
        player_name,
        team_id,
        position,
        class_year,
        height_inches,
        hometown
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (player_name, team_id)
    DO UPDATE SET
        position = EXCLUDED.position,
        class_year = EXCLUDED.class_year,
        height_inches = EXCLUDED.height_inches,
        hometown = EXCLUDED.hometown
    RETURNING *;
    """

    values = (
        data["player_name"],
        data["team_id"],
        data.get("position"),
        data.get("class_year"),
        data.get("height_inches"),
        data.get("hometown"),
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, values)
            result = cur.fetchone()

    return jsonify(result)


# ------------------------
# Player Season Stats
# ------------------------
@app.post("/player-season-stats")
def upsert_player_season_stats():
    data = request.json

    query = """
    INSERT INTO player_season_stats (
        player_id,
        season_year,
        minutes_played,
        two_point_attempts,
        two_point_made,
        three_point_attempts,
        three_point_made,
        free_throw_attempts,
        free_throw_made,
        total_assists,
        offensive_rebounds,
        defensive_rebounds,
        steals,
        blocks,
        total_defensive_fouls,
        total_offensive_fouls,
        total_turnovers
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (player_id, season_year)
    DO UPDATE SET
        minutes_played = EXCLUDED.minutes_played,
        two_point_attempts = EXCLUDED.two_point_attempts,
        two_point_made = EXCLUDED.two_point_made,
        three_point_attempts = EXCLUDED.three_point_attempts,
        three_point_made = EXCLUDED.three_point_made,
        free_throw_attempts = EXCLUDED.free_throw_attempts,
        free_throw_made = EXCLUDED.free_throw_made,
        total_assists = EXCLUDED.total_assists,
        offensive_rebounds = EXCLUDED.offensive_rebounds,
        defensive_rebounds = EXCLUDED.defensive_rebounds,
        steals = EXCLUDED.steals,
        blocks = EXCLUDED.blocks,
        total_defensive_fouls = EXCLUDED.total_defensive_fouls,
        total_offensive_fouls = EXCLUDED.total_offensive_fouls,
        total_turnovers = EXCLUDED.total_turnovers
    RETURNING *;
    """

    values = (
        data["player_id"],
        data["season_year"],
        data["minutes_played"],
        data["two_point_attempts"],
        data["two_point_made"],
        data["three_point_attempts"],
        data["three_point_made"],
        data["free_throw_attempts"],
        data["free_throw_made"],
        data["total_assists"],
        data["offensive_rebounds"],
        data["defensive_rebounds"],
        data["steals"],
        data["blocks"],
        data["total_defensive_fouls"],
        data["total_offensive_fouls"],
        data["total_turnovers"],
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, values)
            result = cur.fetchone()

    return jsonify(result)


@app.get("/health")
def health():
    return {"status": "ok"}