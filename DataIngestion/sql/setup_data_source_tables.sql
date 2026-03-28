CREATE TABLE IF NOT EXISTS conferences (
    id BIGSERIAL PRIMARY KEY,
    conference_name TEXT NOT NULL UNIQUE,
    short_name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS teams (
    id BIGSERIAL PRIMARY KEY,
    team_name TEXT NOT NULL UNIQUE,
    conference_id BIGINT NOT NULL REFERENCES conferences(id) ON DELETE RESTRICT,
    regular_season_record TEXT NOT NULL,
    regular_season_wins INTEGER NOT NULL CHECK (regular_season_wins >= 0),
    regular_season_losses INTEGER NOT NULL CHECK (regular_season_losses >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (regular_season_wins + regular_season_losses > 0)
);

CREATE TABLE IF NOT EXISTS players (
    id BIGSERIAL PRIMARY KEY,
    player_name TEXT NOT NULL,
    team_id BIGINT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    position TEXT,
    class_year TEXT,
    height_inches INTEGER CHECK (height_inches IS NULL OR height_inches > 0),
    hometown TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_name, team_id)
);

CREATE TABLE IF NOT EXISTS player_season_stats (
    id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    season_year INTEGER NOT NULL,
    minutes_played INTEGER NOT NULL CHECK (minutes_played >= 0),
    two_point_attempts INTEGER NOT NULL CHECK (two_point_attempts >= 0),
    two_point_made INTEGER NOT NULL CHECK (two_point_made >= 0),
    three_point_attempts INTEGER NOT NULL CHECK (three_point_attempts >= 0),
    three_point_made INTEGER NOT NULL CHECK (three_point_made >= 0),
    free_throw_attempts INTEGER NOT NULL CHECK (free_throw_attempts >= 0),
    free_throw_made INTEGER NOT NULL CHECK (free_throw_made >= 0),
    total_assists INTEGER NOT NULL CHECK (total_assists >= 0),
    offensive_rebounds INTEGER NOT NULL CHECK (offensive_rebounds >= 0),
    defensive_rebounds INTEGER NOT NULL CHECK (defensive_rebounds >= 0),
    steals INTEGER NOT NULL CHECK (steals >= 0),
    blocks INTEGER NOT NULL CHECK (blocks >= 0),
    total_defensive_fouls INTEGER NOT NULL CHECK (total_defensive_fouls >= 0),
    total_offensive_fouls INTEGER NOT NULL CHECK (total_offensive_fouls >= 0),
    total_turnovers INTEGER NOT NULL CHECK (total_turnovers >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, season_year),
    CHECK (two_point_made <= two_point_attempts),
    CHECK (three_point_made <= three_point_attempts),
    CHECK (free_throw_made <= free_throw_attempts)
);

CREATE INDEX IF NOT EXISTS idx_teams_conference_id
    ON teams(conference_id);

CREATE INDEX IF NOT EXISTS idx_players_team_id
    ON players(team_id);

CREATE INDEX IF NOT EXISTS idx_players_name
    ON players(player_name);

CREATE INDEX IF NOT EXISTS idx_player_season_stats (
    ON player_season_stats(player_id)
)