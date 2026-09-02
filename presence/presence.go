// Package presence implements account device discovery and no-code pairing.
package presence

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"sync"
	"time"

	"github.com/Aruelius/quickdrop-go/api"
	"github.com/gorilla/websocket"
)

type OnlineInstance struct {
	InstanceID  string    `json:"instanceId"`
	DeviceID    string    `json:"deviceId"`
	Label       string    `json:"label"`
	Platform    string    `json:"platform"`
	ConnectedAt time.Time `json:"connectedAt"`
}

type PresenceMessage struct {
	Version          int              `json:"version"`
	Type             string           `json:"type"`
	Instances        []OnlineInstance `json:"instances,omitempty"`
	RequestID        string           `json:"requestId,omitempty"`
	TargetInstanceID string           `json:"targetInstanceId,omitempty"`
	RemoteInstanceID string           `json:"remoteInstanceId,omitempty"`
	From             OnlineInstance   `json:"from,omitempty"`
	GroupID          string           `json:"groupId,omitempty"`
	Initiator        bool             `json:"initiator,omitempty"`
	Credentials      api.Credentials  `json:"credentials,omitempty"`
	Code             string           `json:"code,omitempty"`
	Message          string           `json:"message,omitempty"`
}

type Presence struct {
	conn      *websocket.Conn
	writeMu   sync.Mutex
	messages  chan PresenceMessage
	errors    chan error
	done      chan struct{}
	closeOnce sync.Once
}

func Dial(ctx context.Context, c *api.Client, instance OnlineInstance) (*Presence, error) {
	query := url.Values{"instanceId": {instance.InstanceID}, "deviceId": {instance.DeviceID}, "label": {instance.Label}, "platform": {instance.Platform}}
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, response, err := dialer.DialContext(ctx, c.WebSocketURL("/ws/account", query), c.WebSocketHeader())
	if err != nil {
		if response != nil {
			return nil, errors.New(response.Status)
		}
		return nil, err
	}
	p := &Presence{conn: conn, messages: make(chan PresenceMessage, 32), errors: make(chan error, 1), done: make(chan struct{})}
	conn.SetReadLimit(64 * 1024)
	go p.readLoop()
	go p.heartbeatLoop()
	return p, nil
}

func (p *Presence) Messages() <-chan PresenceMessage { return p.messages }
func (p *Presence) Errors() <-chan error             { return p.errors }

func (p *Presence) Request(targetInstanceID, groupID string) error {
	if groupID == "" {
		groupID = randomID()
	}
	return p.send(map[string]any{"version": 1, "type": "connect_request", "targetInstanceId": targetInstanceID, "groupId": groupID})
}

func randomID() string {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return hex.EncodeToString([]byte(time.Now().String()))
	}
	return hex.EncodeToString(buffer)
}

func (p *Presence) Respond(requestID string, accepted bool) error {
	return p.send(map[string]any{"version": 1, "type": "connect_response", "requestId": requestID, "accepted": accepted})
}

func (p *Presence) Close() error {
	var result error
	p.closeOnce.Do(func() {
		close(p.done)
		p.writeMu.Lock()
		_ = p.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client closed"), time.Now().Add(time.Second))
		result = p.conn.Close()
		p.writeMu.Unlock()
	})
	return result
}

func (p *Presence) send(value any) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	_ = p.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return p.conn.WriteJSON(value)
}

func (p *Presence) readLoop() {
	defer close(p.messages)
	for {
		kind, payload, err := p.conn.ReadMessage()
		if err != nil {
			select {
			case p.errors <- err:
			default:
			}
			return
		}
		if kind != websocket.TextMessage {
			continue
		}
		var message PresenceMessage
		if json.Unmarshal(payload, &message) != nil || message.Version != 1 || message.Type == "" {
			continue
		}
		select {
		case p.messages <- message:
		default:
		}
	}
}

func (p *Presence) heartbeatLoop() {
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := p.send(map[string]any{"version": 1, "type": "heartbeat"}); err != nil {
				return
			}
		case <-p.done:
			return
		}
	}
}
