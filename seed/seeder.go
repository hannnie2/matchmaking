package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

const (
	NUM_PLAYERS = 5000
)

func main() {
	godotenv.Load()

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_NAME"))

	conn, err := pgx.Connect(context.Background(), connStr)

	if err != nil {
		fmt.Fprintf(os.Stdout, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Connected to database\n")

	defer conn.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	tx, err := conn.Begin(ctx)
	if err != nil {
		fmt.Fprintf(os.Stdout, "Error starting transaction: %v\n", err)
		return
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
	WITH new_players AS (
		INSERT INTO players (name)
		SELECT 'Test Player ' || i
		FROM generate_series(1, $1) AS i
		RETURNING id
	)
	INSERT INTO player_ratings (player_id, mode, rating)
	SELECT
		id,
		m.mode,
		(FLOOR(RANDOM() * 301) + 1000)::int
	FROM new_players
	CROSS JOIN (VALUES ('ranked'::match_mode), ('casual'::match_mode)) AS m(mode)
`, NUM_PLAYERS)
	if err != nil {
		fmt.Fprintf(os.Stdout, "Failed to create test data: %v\n", err)
		return
	}

	if err = tx.Commit(ctx); err != nil {
		fmt.Fprintf(os.Stdout, "Failed to commit transaction: %v\n", err)
		return
	}

	fmt.Printf("Successfully created %d test players\n", NUM_PLAYERS)
}
