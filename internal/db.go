package chat
 
import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"
 
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)
 
const (
	maxMessageLen    = 2000
	defaultLimit     = 50
	maxLimit         = 100
	defaultRecent    = 20
	maxRecent        = 50
	dbConnectTimeout = 10 * time.Second
)
 
// Message — xabar modeli
type Message struct {
	ID         int       `json:"id"`
	SenderID   int       `json:"sender_id"`
	ReceiverID int       `json:"receiver_id"`
	Content    string    `json:"message"`
	CreatedAt  time.Time `json:"created_at"`
}
 
// DB — PostgreSQL connection pool wrapper
type DB struct {
	pool *pgxpool.Pool
}
 
// NewDB — DATABASE_URL dan connection pool yaratadi va ping tekshiradi
func NewDB(ctx context.Context) (*DB, error) {
    dsn := os.Getenv("DATABASE_URL")	
	if dsn == "" {
		return nil, errors.New("DATABASE_URL environment variable is required")
	}
 
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
 
	cfg.MaxConns = 25
	cfg.MinConns = 5
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.HealthCheckPeriod = 1 * time.Minute
 
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}
 
	pingCtx, cancel := context.WithTimeout(ctx, dbConnectTimeout)
	defer cancel()
 
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
 
	log.Println("[db] connected to PostgreSQL")
	return &DB{pool: pool}, nil
}
 
// Close — connection poolni yopadi
func (db *DB) Close() {
	db.pool.Close()
	log.Println("[db] connection pool closed")
}
 
// InsertMessage — yangi xabarni saqlaydi va to'liq modelni qaytaradi
func (db *DB) InsertMessage(ctx context.Context, senderID, receiverID int, content string) (*Message, error) {
	if len(content) > maxMessageLen {
		content = content[:maxMessageLen]
	}
 
	var msg Message
	err := db.pool.QueryRow(ctx,
		`INSERT INTO messages (sender_id, receiver_id, message)
		 VALUES ($1, $2, $3)
		 RETURNING id, sender_id, receiver_id, message, created_at`,
		senderID, receiverID, content,
	).Scan(&msg.ID, &msg.SenderID, &msg.ReceiverID, &msg.Content, &msg.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert message: %w", err)
	}
	return &msg, nil
}
 
// GetConversation — ikki foydalanuvchi o'rtasidagi xabarlarni qaytaradi (DESC)
func (db *DB) GetConversation(ctx context.Context, userID, otherID, limit, offset int) ([]Message, error) {
	limit = clamp(limit, 1, maxLimit, defaultLimit)
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
		userID, otherID, limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("query conversation: %w", err)
	}
	return scanMessages(rows)
}
 
// GetRecentConversations — foydalanuvchining so'nggi suhbatlarini qaytaradi
func (db *DB) GetRecentConversations(ctx context.Context, userID, limit int) ([]Message, error) {
	limit = clamp(limit, 1, maxRecent, defaultRecent)
 
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
		return nil, fmt.Errorf("query recent conversations: %w", err)
	}
	return scanMessages(rows)
}
 
func scanMessages(rows pgx.Rows) ([]Message, error) {
	defer rows.Close()
 
	var messages []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.SenderID, &m.ReceiverID, &m.Content, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		messages = append(messages, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return messages, nil
}
 
func clamp(val, minVal, maxVal, def int) int {
	if val <= 0 {
		return def
	}
	if val > maxVal {
		return maxVal
	}
	if val < minVal {
		return minVal
	}
	return val
}
 
