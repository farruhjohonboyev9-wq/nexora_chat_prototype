package main

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
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 8192
	sendBufSize    = 256
	closeGrace     = 2 * time.Second
)

type IncomingMessage struct {
	Type       string `json:"type"`
	ReceiverID int    `json:"receiver_id"`
	Message    string `json:"message"`
}

type OutgoingMessage struct {
	Type       string `json:"type"`
	ID         int    `json:"id,omitempty"`
	SenderID   int    `json:"sender_id"`
	ReceiverID int    `json:"receiver_id,omitempty"`
	Message    string `json:"message,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
	UserID     int    `json:"user_id,omitempty"`
	Online     *bool  `json:"online,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

type routedMessage struct {
	msg          *OutgoingMessage
	originClient *Client
}

type Client struct {
	hub     *Hub
	conn    *websocket.Conn
	send    chan *OutgoingMessage
	userID  int
	limiter *RateLimiter
}

func (c *Client) readPump() {
	defer func() {
		c.gracefulClose()
		c.hub.unregister <- c
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[ws] read error user=%d err=%v", c.userID, err)
			}
			break
		}

		var incoming IncomingMessage
		if err := json.Unmarshal(raw, &incoming); err != nil {
			c.sendError("invalid message format")
			continue
		}

		if !c.limiter.Allow(c.userID) {
			c.sendError("rate limit exceeded")
			continue
		}

		switch incoming.Type {
		case "message":
			c.handleChatMessage(incoming)
		case "typing":
			c.handleTyping(incoming)
		default:
			c.sendError("unknown message type")
		}
	}
}

func (c *Client) handleChatMessage(incoming IncomingMessage) {
	if incoming.ReceiverID <= 0 || incoming.Message == "" {
		c.sendError("receiver_id and message are required")
		return
	}
	if incoming.ReceiverID == c.userID {
		c.sendError("cannot send message to yourself")
		return
	}
	if len(incoming.Message) > maxMessageLen {
		c.sendError("message too long")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := c.hub.db.InsertMessage(ctx, c.userID, incoming.ReceiverID, incoming.Message)
	if err != nil {
		log.Printf("[db] insert failed user=%d err=%v", c.userID, err)
		c.sendError("failed to send message")
		return
	}

	outgoing := &OutgoingMessage{
		Type:       "message",
		ID:         msg.ID,
		SenderID:   msg.SenderID,
		ReceiverID: msg.ReceiverID,
		Message:    msg.Message,
		CreatedAt:  msg.CreatedAt.Format(time.RFC3339),
	}

	c.hub.broadcast <- &routedMessage{msg: outgoing, originClient: c}
}

func (c *Client) handleTyping(incoming IncomingMessage) {
	if incoming.ReceiverID <= 0 || incoming.ReceiverID == c.userID {
		return
	}

	outgoing := &OutgoingMessage{
		Type:       "typing",
		SenderID:   c.userID,
		ReceiverID: incoming.ReceiverID,
	}

	c.hub.broadcast <- &routedMessage{msg: outgoing, originClient: c}
}

func (c *Client) sendError(reason string) {
	select {
	case c.send <- &OutgoingMessage{Type: "error", Reason: reason}:
	default:
	}
}

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
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}

			data, err := json.Marshal(msg)
			if err != nil {
				log.Printf("[ws] marshal error user=%d err=%v", c.userID, err)
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Printf("[ws] write error user=%d err=%v", c.userID, err)
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

func (c *Client) gracefulClose() {
	c.conn.SetWriteDeadline(time.Now().Add(closeGrace))
	c.conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	time.Sleep(50 * time.Millisecond)
	c.conn.Close()
}
