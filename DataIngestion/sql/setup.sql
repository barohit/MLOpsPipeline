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

CREATE INDEX IF NOT EXISTS idx_teams_conference_id
    ON teams(conference_id);

CREATE INDEX IF NOT EXISTS idx_players_team_id
    ON players(team_id);

CREATE INDEX IF NOT EXISTS idx_players_name
    ON players(player_name);