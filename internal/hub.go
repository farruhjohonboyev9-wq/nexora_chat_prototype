package main

import (
	"log"
	"sync"
	"sync/atomic"
)

type Hub struct {
	mu         sync.RWMutex
	clients    map[int]map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan *routedMessage
	db         *DB

	totalConns atomic.Int64
	totalMsgs  atomic.Int64
}

func NewHub(db *DB) *Hub {
	return &Hub{
		clients:    make(map[int]map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan *routedMessage, 512),
		db:         db,
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			if h.clients[client.userID] == nil {
				h.clients[client.userID] = make(map[*Client]bool)
			}
			h.clients[client.userID][client] = true
			h.totalConns.Add(1)
			h.mu.Unlock()
			log.Printf("[hub] user=%d connected (user_conns=%d total_conns=%d)",
				client.userID, len(h.clients[client.userID]), h.totalConns.Load())

			h.notifyOnlineStatus(client.userID, true)

		case client := <-h.unregister:
			h.mu.Lock()
			if clients, ok := h.clients[client.userID]; ok {
				if _, exists := clients[client]; exists {
					delete(clients, client)
					close(client.send)
					h.totalConns.Add(-1)
					if len(clients) == 0 {
						delete(h.clients, client.userID)
						h.mu.Unlock()
						log.Printf("[hub] user=%d disconnected (no remaining conns)", client.userID)
						h.notifyOnlineStatus(client.userID, false)
						continue
					}
				}
			}
			h.mu.Unlock()
			log.Printf("[hub] user=%d disconnected (total_conns=%d)", client.userID, h.totalConns.Load())

		case rm := <-h.broadcast:
			h.totalMsgs.Add(1)
			h.deliver(rm)
		}
	}
}

func (h *Hub) deliver(rm *routedMessage) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	msg := rm.msg

	if clients, ok := h.clients[msg.ReceiverID]; ok {
		for client := range clients {
			select {
			case client.send <- msg:
			default:
				go func(c *Client) {
					h.unregister <- c
				}(client)
			}
		}
	}

	if clients, ok := h.clients[msg.SenderID]; ok {
		for client := range clients {
			if client == rm.originClient {
				continue
			}
			select {
			case client.send <- msg:
			default:
				go func(c *Client) {
					h.unregister <- c
				}(client)
			}
		}
	}
}

func (h *Hub) notifyOnlineStatus(userID int, online bool) {
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
		for client := range clients {
			select {
			case client.send <- msg:
			default:
			}
		}
	}
}

func (h *Hub) IsUserOnline(userID int) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	clients, ok := h.clients[userID]
	return ok && len(clients) > 0
}

func (h *Hub) OnlineUserIDs() []int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ids := make([]int, 0, len(h.clients))
	for id := range h.clients {
		ids = append(ids, id)
	}
	return ids
}

func (h *Hub) ConnectionCount() int {
	return int(h.totalConns.Load())
}

func (h *Hub) MessageCount() int64 {
	return h.totalMsgs.Load()
}
