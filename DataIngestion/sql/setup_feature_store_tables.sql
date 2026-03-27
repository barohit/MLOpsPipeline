CREATE TABLE IF NOT EXISTS team_win_percentage_features (
    team_id BIGINT PRIMARY KEY,
    team_name TEXT NOT NULL,
    conference_id BIGINT NOT NULL,
    regular_season_wins INTEGER NOT NULL,
    regular_season_losses INTEGER NOT NULL,
    win_percentage DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS team_player_count_features (
    team_id BIGINT PRIMARY KEY,
    player_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS conference_team_count_features (
    conference_id BIGINT PRIMARY KEY,
    team_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS conference_win_rate_features (
    conference_id BIGINT PRIMARY KEY,
    total_wins INTEGER NOT NULL,
    total_losses INTEGER NOT NULL,
    conference_win_percentage DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);