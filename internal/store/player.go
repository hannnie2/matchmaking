package store

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// UpsertPlayerRating ensures a player_ratings row exists for the given mode,
// inserting the schema default rating (1000) if none exists yet.
func (s *Store) UpsertPlayerRating(ctx context.Context, playerID int32, mode string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO player_ratings (player_id, mode) VALUES ($1, $2) ON CONFLICT (player_id, mode) DO NOTHING`,
		playerID, mode,
	)
	return err
}

type PlayerWithRating struct{
	Id int32
	Name string
	Rating int32
}

func (s *Store) GetPlayer(ctx context.Context, playerID int32, mode string) (*PlayerWithRating, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT p.id, p.name, pr.rating
		FROM players p
		JOIN player_ratings pr ON p.id = pr.player_id
		WHERE p.id = $1
		AND pr.mode = $2`,
		playerID, mode,
	)
	var p PlayerWithRating
	err := row.Scan(&p.Id, &p.Name, &p.Rating)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}
