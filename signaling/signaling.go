// Package signaling implements the QuickDrop peer signaling WebSocket.
package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"sync"
	"time"

	"github.com/Aruelius/quickdrop-go/api"
	"github.com/gorilla/websocket"
)

type Signal struct {
	Version int             `json:"version"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Code    string          `json:"code,omitempty"`
	Message string          `json:"message,omitempty"`
}

type Signaling struct {
	conn      *websocket.Conn
	writeMu   sync.Mutex
	messages  chan Signal
	errors    chan error
	closeOnce sync.Once
}

func Dial(ctx context.Context, c *api.Client, credentials api.Credentials) (*Signaling, error) {
	query := url.Values{"sessionId": {credentials.SessionID}, "peerId": {credentials.PeerID}, "peerToken": {credentials.PeerToken}}
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, response, err := dialer.DialContext(ctx, c.WebSocketURL("/ws", query), c.WebSocketHeader())
	if err != nil {
		if response != nil {
			return nil, errors.New(response.Status)
		}
		return nil, err
	}
	s := &Signaling{conn: conn, messages: make(chan Signal, 32), errors: make(chan error, 1)}
	conn.SetReadLimit(128 * 1024)
	go s.readLoop()
	return s, nil
}

func (s *Signaling) Messages() <-chan Signal { return s.messages }
func (s *Signaling) Errors() <-chan error    { return s.errors }

func (s *Signaling) Send(kind string, payload any) error {
	value := map[string]any{"version": 1, "type": kind}
	if payload != nil {
		value["payload"] = payload
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return s.conn.WriteJSON(value)
}

func (s *Signaling) Close() error {
	var result error
	s.closeOnce.Do(func() {
		s.writeMu.Lock()
		_ = s.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client closed"), time.Now().Add(time.Second))
		result = s.conn.Close()
		s.writeMu.Unlock()
	})
	return result
}

func (s *Signaling) readLoop() {
	defer close(s.messages)
	for {
		kind, payload, err := s.conn.ReadMessage()
		if err != nil {
			select {
			case s.errors <- err:
			default:
			}
			return
		}
		if kind != websocket.TextMessage {
			continue
		}
		var message Signal
		if json.Unmarshal(payload, &message) != nil || message.Version != 1 || message.Type == "" {
			continue
		}
		select {
		case s.messages <- message:
		default:
		}
	}
}
