package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		allowedOrigins := os.Getenv("ALLOWED_ORIGINS")
		if allowedOrigins == "" {
			return true
		}
		origin := r.Header.Get("Origin")
		for _, allowed := range strings.Split(allowedOrigins, ",") {
			if strings.TrimSpace(allowed) == origin {
				return true
			}
		}
		return false
	},
}

func extractToken(r *http.Request) (string, error) {
	token := r.URL.Query().Get("token")
	if token != "" {
		return token, nil
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			if t := strings.TrimSpace(parts[1]); t != "" {
				return t, nil
			}
		}
	}

	return "", fmt.Errorf("missing token: provide ?token=JWT query param or Authorization: Bearer header")
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	tokenStr, err := extractToken(r)
	if err != nil {
		log.Printf("[auth] %v", err)
		http.Error(w, "Unauthorized: "+err.Error(), http.StatusUnauthorized)
		return
	}

	userID, err := ValidateJWT(tokenStr)
	if err != nil {
		log.Printf("[auth] JWT invalid: %v", err)
		http.Error(w, "Unauthorized: invalid token", http.StatusUnauthorized)
		return
	}

	if !s.connLimiter.Allow(userID) {
		log.Printf("[auth] user=%d connection rate limit exceeded", userID)
		http.Error(w, "Too Many Connections", http.StatusTooManyRequests)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ws] upgrade error: %v", err)
		return
	}

	client := &Client{
		hub:     s.hub,
		conn:    conn,
		send:    make(chan *OutgoingMessage, sendBufSize),
		userID:  userID,
		limiter: s.limiter,
	}

	s.hub.register <- client

	go client.writePump()
	go client.readPump()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "ok",
		"service":     "nexora-chat",
		"connections": s.hub.ConnectionCount(),
		"messages":    s.hub.MessageCount(),
	})
}

func (s *Server) handleConversation(w http.ResponseWriter, r *http.Request) {
	tokenStr, err := extractToken(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	userID, err := ValidateJWT(tokenStr)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	otherIDStr := r.URL.Query().Get("with")
	otherID, err := strconv.Atoi(otherIDStr)
	if err != nil || otherID <= 0 {
		http.Error(w, "invalid 'with' parameter", http.StatusBadRequest)
		return
	}

	limit := 50
	if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && l > 0 && l <= 100 {
		limit = l
	}

	offset := 0
	if o, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil && o >= 0 {
		offset = o
	}

	messages, err := s.db.GetConversation(r.Context(), userID, otherID, limit, offset)
	if err != nil {
		log.Printf("[db] conversation query failed user=%d other=%d err=%v", userID, otherID, err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if messages == nil {
		messages = []Message{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(messages)
}
