package store

import (
	"context"
	"matchmaking/internal/model"
	"time"
)

func (s *Store) InsertMatch(ctx context.Context, m *model.Match) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO matches (id, region, mode, rating_band, status, player_ids, formed_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (id) DO NOTHING`,
		m.ID, m.Shard.Region, m.Shard.Mode, m.Shard.RatingBand,
		string(m.Status), m.PlayerIDs, m.FormedAt,
	)
	return err
}

func (s *Store) MarkMatchConfirmed(ctx context.Context, matchID string, at time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE matches SET status = 'confirmed', confirmed_at = $2 WHERE id = $1`,
		matchID, at,
	)
	return err
}

func (s *Store) MarkMatchDissolved(ctx context.Context, matchID string, at time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE matches SET status = 'dissolved', dissolved_at = $2 WHERE id = $1`,
		matchID, at,
	)
	return err
}

func (s *Store) SetMatchServerAddr(ctx context.Context, matchID, serverAddr string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE matches SET server_addr = $2 WHERE id = $1`,
		matchID, serverAddr,
	)
	return err
}

func (s *Store) InsertRatingHistory(ctx context.Context,
	playerID, matchID string,
	muBefore, sigmaBefore, muAfter, sigmaAfter float64,
) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO rating_history
		 (player_id, match_id, mu_before, sigma_before, mu_after, sigma_after)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		playerID, matchID, muBefore, sigmaBefore, muAfter, sigmaAfter,
	)
	return err
}
