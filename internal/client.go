package chat
 
import (
	"context"
	"encoding/json"
	"log"
	"time"
 
	"github.com/gorilla/websocket"
)
 
const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = pongWait * 9 / 10
	maxMessageSize = 8192
	sendBufSize    = 256
	closeGrace     = 2 * time.Second
)
 
// IncomingMessage — clientdan kelgan xabar
type IncomingMessage struct {
	Type       string `json:"type"`
	ReceiverID int    `json:"receiver_id"`
	Message    string `json:"message"`
}
 
// OutgoingMessage — clientga yuboriladigan xabar
type OutgoingMessage struct {
	Type       string `json:"type"`
	ID         int    `json:"id,omitempty"`
	SenderID   int    `json:"sender_id,omitempty"`
	ReceiverID int    `json:"receiver_id,omitempty"`
	Message    string `json:"message,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
	UserID     int    `json:"user_id,omitempty"`
	Online     *bool  `json:"online,omitempty"`
	Reason     string `json:"reason,omitempty"`
}
 
// routedMessage — Hub ichida ishlatiladi
type routedMessage struct {
	msg          *OutgoingMessage
	originClient *Client
}
 
// Client — bitta WebSocket ulanishini ifodalaydi
type Client struct {
	hub     *Hub
	conn    *websocket.Conn
	send    chan *OutgoingMessage
	userID  int
	limiter *RateLimiter
}
 
// readPump — clientdan xabarlarni o'qiydi.
// Faqat bitta goroutineda ishlatilishi kerak.
func (c *Client) readPump() {
	defer func() {
		c.gracefulClose()
		c.hub.unregister <- c
	}()
 
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})
 
	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
				websocket.CloseNoStatusReceived,
			) {
				log.Printf("[client] read error user=%d: %v", c.userID, err)
			}
			return
		}
 
		var msg IncomingMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			c.sendError("invalid JSON format")
			continue
		}
 
		if !c.limiter.Allow(c.userID) {
			c.sendError("rate limit exceeded, slow down")
			continue
		}
 
		switch msg.Type {
		case "message":
			c.handleChat(msg)
		case "typing":
			c.handleTyping(msg)
		default:
			c.sendError("unknown message type: " + msg.Type)
		}
	}
}
 
// handleChat — chat xabarini validatsiya qilib DB ga saqlaydi va broadcast qiladi
func (c *Client) handleChat(in IncomingMessage) {
	switch {
	case in.ReceiverID <= 0:
		c.sendError("receiver_id is required")
		return
	case in.ReceiverID == c.userID:
		c.sendError("cannot send message to yourself")
		return
	case len(in.Message) == 0:
		c.sendError("message cannot be empty")
		return
	case len(in.Message) > maxMessageLen:
		c.sendError("message too long")
		return
	}
 
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
 
	saved, err := c.hub.db.InsertMessage(ctx, c.userID, in.ReceiverID, in.Message)
	if err != nil {
		log.Printf("[client] db insert failed user=%d: %v", c.userID, err)
		c.sendError("failed to send message, try again")
		return
	}
 
	out := &OutgoingMessage{
		Type:       "message",
		ID:         saved.ID,
		SenderID:   saved.SenderID,
		ReceiverID: saved.ReceiverID,
		Message:    saved.Content,
		CreatedAt:  saved.CreatedAt.Format(time.RFC3339),
	}
 
	c.hub.broadcast <- &routedMessage{msg: out, originClient: c}
}
 
// handleTyping — typing indikatorini receiver ga uzatadi
func (c *Client) handleTyping(in IncomingMessage) {
	if in.ReceiverID <= 0 || in.ReceiverID == c.userID {
		return
	}
	c.hub.broadcast <- &routedMessage{
		msg: &OutgoingMessage{
			Type:       "typing",
			SenderID:   c.userID,
			ReceiverID: in.ReceiverID,
		},
		originClient: c,
	}
}
 
// writePump — clientga xabarlar yuboradi va ping/pong ushlab turadi.
// Faqat bitta goroutineda ishlatilishi kerak.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
 
	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "server closed"))
				return
			}
			data, err := json.Marshal(msg)
			if err != nil {
				log.Printf("[client] marshal error user=%d: %v", c.userID, err)
				continue
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
 
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
 
// sendError — clientga xato xabari yuboradi (non-blocking)
func (c *Client) sendError(reason string) {
	select {
	case c.send <- &OutgoingMessage{Type: "error", Reason: reason}:
	default:
		log.Printf("[client] send buffer full, dropped error for user=%d", c.userID)
	}
}
 
// gracefulClose — WebSocket ni tartibli yopadi
func (c *Client) gracefulClose() {
	deadline := time.Now().Add(closeGrace)
	c.conn.SetWriteDeadline(deadline)
	c.conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	time.Sleep(50 * time.Millisecond)
	c.conn.Close()
}
 
