CREATE TYPE match_mode AS ENUM ('ranked', 'casual');
CREATE TYPE match_status AS ENUM ('in_progress', 'completed');

CREATE TABLE matches (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mode match_mode NOT NULL,
    status match_status NOT NULL DEFAULT 'in_progress',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE TABLE players (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE match_players (
    match_id INT NOT NULL,
    player_id INT NOT NULL,

    PRIMARY KEY (match_id, player_id),

    FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES players(id) ON DELETE CASCADE
);

CREATE TABLE player_ratings (
    player_id INT NOT NULL,
    mode match_mode NOT NULL,
    rating INT NOT NULL DEFAULT 1000,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (player_id, mode),

    FOREIGN KEY (player_id) REFERENCES players(id) ON DELETE CASCADE
);
