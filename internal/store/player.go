package store

import (
	"context"
	"matchmaking/internal/model"

	"github.com/jackc/pgx/v5"
)

// UpsertPlayer ensures a player row exists. Called at queue join time.
func (s *Store) UpsertPlayer(ctx context.Context, playerID string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO players (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`,
		playerID,
	)
	return err
}

func (s *Store) GetPlayer(ctx context.Context, playerID string) (*model.Player, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT id, rating, games_played, created_at, updated_at
		 FROM players WHERE id = $1`,
		playerID,
	)
	var p model.Player
	err := row.Scan(&p.ID, &p.Rating, &p.GamesPlayed, &p.CreatedAt, &p.UpdatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}
