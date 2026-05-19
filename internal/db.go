package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxMessageLen = 2000

type Message struct {
	ID         int       `json:"id"`
	SenderID   int       `json:"sender_id"`
	ReceiverID int       `json:"receiver_id"`
	Message    string    `json:"message"`
	CreatedAt  time.Time `json:"created_at"`
}

type DB struct {
	pool *pgxpool.Pool
}

func NewDB(ctx context.Context) (*DB, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable is required")
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse DATABASE_URL: %w", err)
	}

	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnIdleTime = 5 * time.Minute
	config.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	log.Println("Connected to PostgreSQL")
	return &DB{pool: pool}, nil
}

func (db *DB) Close() {
	db.pool.Close()
}

func (db *DB) InsertMessage(ctx context.Context, senderID, receiverID int, content string) (*Message, error) {
	if len(content) > maxMessageLen {
		content = content[:maxMessageLen]
	}

	var msg Message
	err := db.pool.QueryRow(ctx,
		`INSERT INTO messages (sender_id, receiver_id, message) VALUES ($1, $2, $3) RETURNING id, sender_id, receiver_id, message, created_at`,
		senderID, receiverID, content,
	).Scan(&msg.ID, &msg.SenderID, &msg.ReceiverID, &msg.Message, &msg.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to insert message: %w", err)
	}
	return &msg, nil
}

func (db *DB) GetConversation(ctx context.Context, userID, otherUserID int, limit, offset int) ([]Message, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	rows, err := db.pool.Query(ctx,
		`SELECT id, sender_id, receiver_id, message, created_at
		 FROM messages
		 WHERE (sender_id = $1 AND receiver_id = $2)
		    OR (sender_id = $2 AND receiver_id = $1)
		 ORDER BY created_at DESC
		 LIMIT $3 OFFSET $4`,
		userID, otherUserID, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get conversation: %w", err)
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.ID, &msg.SenderID, &msg.ReceiverID, &msg.Message, &msg.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan message: %w", err)
		}
		messages = append(messages, msg)
	}
	return messages, rows.Err()
}

func (db *DB) GetRecentConversations(ctx context.Context, userID int, limit int) ([]Message, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	rows, err := db.pool.Query(ctx,
		`SELECT DISTINCT ON (other_user) id, sender_id, receiver_id, message, created_at
		 FROM (
		     SELECT id, sender_id, receiver_id, message, created_at,
		            CASE WHEN sender_id = $1 THEN receiver_id ELSE sender_id END AS other_user
		     FROM messages
		     WHERE sender_id = $1 OR receiver_id = $1
		 ) sub
		 ORDER BY other_user, created_at DESC
		 LIMIT $2`,
		userID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent conversations: %w", err)
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.ID, &msg.SenderID, &msg.ReceiverID, &msg.Message, &msg.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan message: %w", err)
		}
		messages = append(messages, msg)
	}
	return messages, rows.Err()
}
