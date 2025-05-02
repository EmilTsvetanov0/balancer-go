package postgresql

import (
	"balancer/internal/domain"
	client2 "balancer/internal/postgresql/client"
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"log"
)

type PgClient struct {
	client client2.Client
	logger *log.Logger
}

// Errors
var ErrClientNotFound = errors.New("client not found")

var ErrClientAlreadyExists = errors.New("client already exists")

func (r *PgClient) GetClient(ctx context.Context, clientId string) (domain.Client, error) {

	query := `
        SELECT capacity, rate
		FROM clients
        WHERE id = $1
	`

	foundClient := domain.Client{Id: clientId}

	err := r.client.QueryRow(ctx, query, clientId).Scan(&foundClient.Capacity, &foundClient.Rate)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Client{}, ErrClientNotFound
		} else {
			return domain.Client{}, fmt.Errorf("GetClient.QueryRow: %w", err)
		}
	}
	return foundClient, nil
}

func (r *PgClient) UpdateClient(ctx context.Context, client domain.Client) error {

	query := `
        UPDATE clients 
        SET capacity = $2, rate = $3
        WHERE id = $1
        RETURNING id`

	var updatedID string

	err := r.client.QueryRow(ctx, query, client.Id, client.Capacity, client.Rate).Scan(&updatedID)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrClientNotFound
		} else {
			return fmt.Errorf("UpdateClient.QueryRow: %w", err)
		}
	}
	return nil
}

func (r *PgClient) DeleteClient(ctx context.Context, clientId string) error {

	query := `
        DELETE FROM clients 
        WHERE id = $1
        RETURNING id
    `

	var deletedID string

	err := r.client.QueryRow(ctx, query, clientId).Scan(&deletedID)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrClientNotFound
		} else {
			return fmt.Errorf("DeleteClient.QueryRow: %w", err)
		}
	}

	return nil
}

func (r *PgClient) InsertClient(ctx context.Context, client domain.Client) error {

	query := `
        INSERT INTO clients (id, capacity, rate)
        VALUES ($1, $2, $3)
        RETURNING id
    `

	var insertedID string

	err := r.client.QueryRow(ctx, query, client.Id, client.Capacity, client.Rate).Scan(&insertedID)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == "23505" {
				return ErrClientAlreadyExists
			}
		}

		return fmt.Errorf("InsertClient.QueryRow: %w", err)
	}

	return nil
}

func NewPgClient(client client2.Client, logger *log.Logger) *PgClient {
	return &PgClient{client, logger}
}
