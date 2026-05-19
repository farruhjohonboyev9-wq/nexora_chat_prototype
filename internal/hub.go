package chat
 
import (
	"log"
	"sync"
	"sync/atomic"
)
 
// Hub — barcha WebSocket ulanishlarni boshqaradi.
// register/unregister/broadcast kanallar orqali ishlaydi (goroutine-safe).
type Hub struct {
	mu         sync.RWMutex
	clients    map[int]map[*Client]bool // userID → connections
	register   chan *Client
	unregister chan *Client
	broadcast  chan *routedMessage
	db         *DB
 
	totalConns atomic.Int64
	totalMsgs  atomic.Int64
}
 
// NewHub — Hub yaratadi
func NewHub(db *DB) *Hub {
	return &Hub{
		clients:    make(map[int]map[*Client]bool),
		register:   make(chan *Client, 16),
		unregister: make(chan *Client, 16),
		broadcast:  make(chan *routedMessage, 512),
		db:         db,
	}
}
 
// Run — Hub event loop (alohida goroutineda ishga tushirilishi kerak)
func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.addClient(c)
 
		case c := <-h.unregister:
			h.removeClient(c)
 
		case rm := <-h.broadcast:
			h.totalMsgs.Add(1)
			h.deliver(rm)
		}
	}
}
 
func (h *Hub) addClient(c *Client) {
	h.mu.Lock()
	if h.clients[c.userID] == nil {
		h.clients[c.userID] = make(map[*Client]bool)
	}
	h.clients[c.userID][c] = true
	count := len(h.clients[c.userID])
	h.totalConns.Add(1)
	h.mu.Unlock()
 
	log.Printf("[hub] user=%d connected (tabs=%d total=%d)", c.userID, count, h.totalConns.Load())
	h.notifyStatus(c.userID, true)
}
 
func (h *Hub) removeClient(c *Client) {
	h.mu.Lock()
	conns, ok := h.clients[c.userID]
	if !ok {
		h.mu.Unlock()
		return
	}
	if _, exists := conns[c]; !exists {
		h.mu.Unlock()
		return
	}
 
	delete(conns, c)
	close(c.send)
	h.totalConns.Add(-1)
	total := h.totalConns.Load()
 
	if len(conns) == 0 {
		delete(h.clients, c.userID)
		h.mu.Unlock()
		log.Printf("[hub] user=%d fully disconnected (total=%d)", c.userID, total)
		h.notifyStatus(c.userID, false)
		return
	}
	h.mu.Unlock()
	log.Printf("[hub] user=%d tab closed (tabs=%d total=%d)", c.userID, len(conns), total)
}
 
// deliver — xabarni receiver va sender (boshqa tablar)ga yetkazadi
func (h *Hub) deliver(rm *routedMessage) {
	h.mu.RLock()
	defer h.mu.RUnlock()
 
	h.sendToUser(rm.msg.ReceiverID, nil, rm.msg)
	h.sendToUser(rm.msg.SenderID, rm.originClient, rm.msg)
}
 
// sendToUser — userID ga tegishli barcha clientlarga xabar yuboradi.
// skip client bo'lsa u o'tkazib yuboriladi (xabar yuborganning o'zi).
func (h *Hub) sendToUser(userID int, skip *Client, msg *OutgoingMessage) {
	for c := range h.clients[userID] {
		if c == skip {
			continue
		}
		select {
		case c.send <- msg:
		default:
			// Bufer to'lgan — async unregister
			go func(c *Client) { h.unregister <- c }(c)
		}
	}
}
 
// notifyStatus — foydalanuvchi online/offline bo'lganini boshqalarga xabar qiladi
func (h *Hub) notifyStatus(userID int, online bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
 
	msg := &OutgoingMessage{
		Type:   "online_status",
		UserID: userID,
		Online: &online,
	}
 
	for uid, clients := range h.clients {
		if uid == userID {
			continue
		}
		for c := range clients {
			select {
			case c.send <- msg:
			default:
			}
		}
	}
}
 
// IsOnline — foydalanuvchi hozir ulanganmi?
func (h *Hub) IsOnline(userID int) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients[userID]) > 0
}
 
// OnlineUsers — hozir ulangan foydalanuvchilar ID lari
func (h *Hub) OnlineUsers() []int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]int, 0, len(h.clients))
	for id := range h.clients {
		ids = append(ids, id)
	}
	return ids
}
 
// Stats — monitoring uchun
func (h *Hub) Stats() (conns int64, msgs int64) {
	return h.totalConns.Load(), h.totalMsgs.Load()
}
 
