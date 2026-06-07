package store

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// GetRating returns a player's rating for a mode. found is false (with a nil
// error) when no player_ratings row exists for the player+mode pair. This is
// the system-of-record read behind the cache-aside rating cache.
func (s *Store) GetRating(ctx context.Context, playerID int32, mode string) (rating int32, found bool, err error) {
	row := s.pool.QueryRow(ctx,
		`SELECT rating FROM player_ratings WHERE player_id = $1 AND mode = $2`,
		playerID, mode,
	)
	if err := row.Scan(&rating); err != nil {
		if err == pgx.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, err
	}
	return rating, true, nil
}
