-- Make randomness reproducible
SELECT setseed(0.42);

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
SELECT
    p.id AS player_id,
    2026 AS season_year,

    minutes_played,

    two_point_attempts,

    LEAST(
        two_point_attempts,
        FLOOR(two_point_attempts * (0.42 + random() * 0.20))::INT
    ) AS two_point_made,

    three_point_attempts,

    LEAST(
        three_point_attempts,
        FLOOR(three_point_attempts * (0.28 + random() * 0.18))::INT
    ) AS three_point_made,

    free_throw_attempts,

    LEAST(
        free_throw_attempts,
        FLOOR(free_throw_attempts * (0.62 + random() * 0.25))::INT
    ) AS free_throw_made,

    FLOOR(minutes_played * (0.03 + random() * 0.07))::INT AS total_assists,
    FLOOR(minutes_played * (0.015 + random() * 0.045))::INT AS offensive_rebounds,
    FLOOR(minutes_played * (0.03 + random() * 0.07))::INT AS defensive_rebounds,
    FLOOR(minutes_played * (0.008 + random() * 0.025))::INT AS steals,
    FLOOR(minutes_played * (0.005 + random() * 0.02))::INT AS blocks,
    FLOOR(minutes_played * (0.015 + random() * 0.03))::INT AS total_defensive_fouls,
    FLOOR(minutes_played * (0.003 + random() * 0.012))::INT AS total_offensive_fouls,
    FLOOR(minutes_played * (0.02 + random() * 0.05))::INT AS total_turnovers

FROM (
    SELECT
        id,
        FLOOR(250 + random() * 850)::INT AS minutes_played,
        FLOOR(80 + random() * 220)::INT AS two_point_attempts,
        FLOOR(20 + random() * 180)::INT AS three_point_attempts,
        FLOOR(25 + random() * 120)::INT AS free_throw_attempts
    FROM players
) p

ON CONFLICT (player_id, season_year) DO UPDATE SET
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
    total_turnovers = EXCLUDED.total_turnovers;