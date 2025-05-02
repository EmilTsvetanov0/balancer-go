package client

import (
	"balancer/internal/utils"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/viper"
	"log"
	"time"
)

type Client interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

func NewClient(ctx context.Context) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", viper.GetString("postgres.user"), viper.GetString("postgres.password"), viper.GetString("postgres.host"), viper.GetString("postgres.port"), viper.GetString("postgres.dbname"))
	var pool *pgxpool.Pool
	var err error
	maxAttempts := viper.GetInt("postgres.max_attempts")

	err = utils.DoWithTries(func() error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		pool, err = pgxpool.New(ctx, dsn)
		return err
	}, maxAttempts, 5*time.Second)

	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL after %d attempts: %v", maxAttempts, err)
		return nil, err
	}

	log.Printf("Connected to PostgreSQL after %d attempts", maxAttempts)

	return pool, nil
}
