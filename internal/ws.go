
Copy

package chat
 
import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
 
	"github.com/gorilla/websocket"
)
 
// upgrader — HTTP → WebSocket upgrade sozlamalari
var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	HandshakeTimeout: 10 * time.Second,
	CheckOrigin: func(r *http.Request) bool {
		allowed := os.Getenv("ALLOWED_ORIGINS")
		if allowed == "" {
			return true // dev: hammaga ruxsat
		}
		origin := r.Header.Get("Origin")
		for _, o := range strings.Split(allowed, ",") {
			if strings.EqualFold(strings.TrimSpace(o), origin) {
				return true
			}
		}
		return false
	},
}
 
// Server — HTTP server va handler lar
type Server struct {
	hub         *Hub
	db          *DB
	Mux         *http.ServeMux
	limiter     *RateLimiter
	connLimiter *ConnLimiter
}
 
// NewServer — Server yaratadi va route larni ro'yxatdan o'tkazadi
func NewServer(hub *Hub, db *DB, limiter *RateLimiter, connLimiter *ConnLimiter) *Server {
	s := &Server{
		hub:         hub,
		db:          db,
		Mux:         http.NewServeMux(),
		limiter:     limiter,
		connLimiter: connLimiter,
	}
	s.Mux.HandleFunc("/ws/chat", s.handleWebSocket)
	s.Mux.HandleFunc("/health", s.handleHealth)
	s.Mux.HandleFunc("/api/conversation", s.handleConversation)
	return s
}
 
// ─── Handlers ────────────────────────────────────────────────
 
// handleWebSocket — JWT tekshirib, WebSocket ulanishini qabul qiladi
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	token, err := extractToken(r)
	if err != nil {
		httpError(w, err.Error(), http.StatusUnauthorized)
		return
	}
 
	userID, err := ValidateJWT(token)
	if err != nil {
		log.Printf("[ws] auth failed: %v", err)
		httpError(w, "invalid or expired token", http.StatusUnauthorized)
		return
	}
 
	if !s.connLimiter.Allow(userID) {
		log.Printf("[ws] conn limit user=%d", userID)
		httpError(w, "too many connections, try again later", http.StatusTooManyRequests)
		return
	}
 
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ws] upgrade error user=%d: %v", userID, err)
		return
	}
 
	c := &Client{
		hub:     s.hub,
		conn:    conn,
		send:    make(chan *OutgoingMessage, sendBufSize),
		userID:  userID,
		limiter: s.limiter,
	}
 
	s.hub.register <- c
	go c.writePump()
	go c.readPump()
}
 
// handleHealth — liveness probe uchun
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	conns, msgs := s.hub.Stats()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":      "ok",
		"service":     "nexora-chat",
		"connections": conns,
		"messages":    msgs,
		"online_users": len(s.hub.OnlineUsers()),
	})
}
 
// handleConversation — ikki foydalanuvchi o'rtasidagi xabarlarni qaytaradi
func (s *Server) handleConversation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
 
	token, err := extractToken(r)
	if err != nil {
		httpError(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	userID, err := ValidateJWT(token)
	if err != nil {
		httpError(w, "unauthorized", http.StatusUnauthorized)
		return
	}
 
	otherID, err := queryInt(r, "with")
	if err != nil || otherID <= 0 {
		httpError(w, "invalid or missing 'with' parameter", http.StatusBadRequest)
		return
	}
	if otherID == userID {
		httpError(w, "cannot query conversation with yourself", http.StatusBadRequest)
		return
	}
 
	limit, _ := queryInt(r, "limit")
	offset, _ := queryInt(r, "offset")
 
	messages, err := s.db.GetConversation(r.Context(), userID, otherID, limit, offset)
	if err != nil {
		log.Printf("[api] conversation query failed user=%d other=%d: %v", userID, otherID, err)
		httpError(w, "internal server error", http.StatusInternalServerError)
		return
	}
 
	if messages == nil {
		messages = []Message{}
	}
	writeJSON(w, http.StatusOK, messages)
}
 
// ─── Helpers ─────────────────────────────────────────────────
 
// extractToken — ?token= yoki Authorization: Bearer headerdan token oladi
func extractToken(r *http.Request) (string, error) {
	if t := r.URL.Query().Get("token"); t != "" {
		return t, nil
	}
	if h := r.Header.Get("Authorization"); h != "" {
		parts := strings.SplitN(h, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			if t := strings.TrimSpace(parts[1]); t != "" {
				return t, nil
			}
		}
	}
	return "", errors.New("missing token: use ?token= or Authorization: Bearer")
}
 
// queryInt — URL query parametrini int ga o'giradi
func queryInt(r *http.Request, key string) (int, error) {
	v := r.URL.Query().Get(key)
	if v == "" {
		return 0, fmt.Errorf("missing param: %s", key)
	}
	return strconv.Atoi(v)
}
 
// httpError — JSON xato javob
func httpError(w http.ResponseWriter, msg string, code int) {
	writeJSON(w, code, map[string]string{"error": msg})
}
 
// writeJSON — JSON javob yozadi
func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[http] json encode error: %v", err)
	}
}
