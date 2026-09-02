// Package api provides the typed HTTP client and shared API models used by
// QuickDrop native clients.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

type Credentials struct {
	SessionID string    `json:"sessionId"`
	PeerID    string    `json:"peerId"`
	PeerToken string    `json:"peerToken"`
	Code      string    `json:"code,omitempty"`
	ExpiresAt time.Time `json:"expiresAt"`
}

type ICEPolicy struct {
	Transport         string `json:"transport"`
	Provider          string `json:"provider,omitempty"`
	AllowFiles        bool   `json:"allowFiles"`
	BandwidthLimitBPS uint64 `json:"bandwidthLimitBps,omitempty"`
}

type ICEServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

type ICEConfiguration struct {
	ICEServers []ICEServer `json:"iceServers"`
	Policy     ICEPolicy   `json:"policy"`
}

type User struct {
	ID          string `json:"id"`
	Username    string `json:"username"`
	DisplayName string `json:"displayName"`
	PlanID      string `json:"planId"`
}

type AuthState struct {
	User                 User `json:"user"`
	CustomTURNConfigured bool `json:"customTurnConfigured"`
}

type DeviceToken struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Token     string    `json:"token,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	LastUsed  time.Time `json:"lastUsedAt,omitempty"`
	ExpiresAt time.Time `json:"expiresAt,omitempty"`
}

type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

type Client struct {
	baseURL *url.URL
	origin  string
	http    *http.Client
	token   string
}

type ClientOptions struct {
	AllowInsecureHTTP bool
}

func New(baseURL, token string) (*Client, error) {
	return NewWithOptions(baseURL, token, ClientOptions{})
}

func NewWithOptions(baseURL, token string, options ClientOptions) (*Client, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, errors.New("server must be an absolute http(s) URL")
	}
	if parsed.Scheme == "http" && !options.AllowInsecureHTTP && !localHTTPHost(parsed.Hostname()) {
		return nil, errors.New("plain HTTP is allowed only for localhost/private IPs; use HTTPS or explicitly allow insecure HTTP")
	}
	parsed.Path, parsed.RawQuery, parsed.Fragment = strings.TrimRight(parsed.Path, "/"), "", ""
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	origin := parsed.Scheme + "://" + parsed.Host
	return &Client{baseURL: parsed, origin: origin, http: &http.Client{Jar: jar, Timeout: 30 * time.Second}, token: strings.TrimSpace(token)}, nil
}

func localHTTPHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address, err := netip.ParseAddr(host)
	return err == nil && (address.IsLoopback() || address.IsPrivate() || address.IsLinkLocalUnicast())
}

func (c *Client) Origin() string        { return c.origin }
func (c *Client) Token() string         { return c.token }
func (c *Client) SetToken(value string) { c.token = strings.TrimSpace(value) }

func (c *Client) CreateSession(ctx context.Context) (Credentials, error) {
	var result Credentials
	err := c.request(ctx, http.MethodPost, "/api/sessions", nil, &result)
	return result, err
}

func (c *Client) JoinSession(ctx context.Context, code string) (Credentials, error) {
	var result Credentials
	err := c.request(ctx, http.MethodPost, "/api/sessions/join", map[string]string{"code": strings.TrimSpace(code)}, &result)
	return result, err
}

func (c *Client) ICE(ctx context.Context, credentials Credentials, mode, provider string) (ICEConfiguration, error) {
	query := url.Values{"sessionId": {credentials.SessionID}, "peerId": {credentials.PeerID}, "peerToken": {credentials.PeerToken}}
	if mode != "" && mode != "direct" {
		query.Set("mode", mode)
	}
	if provider != "" {
		query.Set("provider", provider)
	}
	var result ICEConfiguration
	err := c.request(ctx, http.MethodGet, "/api/ice?"+query.Encode(), nil, &result)
	return result, err
}

func (c *Client) Login(ctx context.Context, username, password string) (AuthState, error) {
	var result AuthState
	err := c.request(ctx, http.MethodPost, "/api/auth/login", map[string]string{"username": username, "password": password}, &result)
	return result, err
}

func (c *Client) Me(ctx context.Context) (AuthState, error) {
	var result AuthState
	err := c.request(ctx, http.MethodGet, "/api/auth/me", nil, &result)
	return result, err
}

func (c *Client) CreateDeviceToken(ctx context.Context, name string) (DeviceToken, error) {
	var result DeviceToken
	err := c.request(ctx, http.MethodPost, "/api/account/device-tokens", map[string]string{"name": name}, &result)
	return result, err
}

func (c *Client) ListDeviceTokens(ctx context.Context) ([]DeviceToken, error) {
	var result struct {
		Tokens []DeviceToken `json:"tokens"`
	}
	err := c.request(ctx, http.MethodGet, "/api/account/device-tokens", nil, &result)
	return result.Tokens, err
}

func (c *Client) DeleteDeviceToken(ctx context.Context, id string) error {
	return c.request(ctx, http.MethodDelete, "/api/account/device-tokens/"+url.PathEscape(id), nil, nil)
}

func (c *Client) WebSocketURL(path string, query url.Values) string {
	result := *c.baseURL
	if result.Scheme == "https" {
		result.Scheme = "wss"
	} else {
		result.Scheme = "ws"
	}
	result.Path = strings.TrimRight(result.Path, "/") + path
	result.RawQuery = query.Encode()
	return result.String()
}

func (c *Client) WebSocketHeader() http.Header {
	header := make(http.Header)
	header.Set("Origin", c.origin)
	if c.token != "" {
		header.Set("Authorization", "Bearer "+c.token)
	}
	for _, cookie := range c.http.Jar.Cookies(c.baseURL) {
		header.Add("Cookie", cookie.String())
	}
	return header
}

func (c *Client) request(ctx context.Context, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + strings.Split(path, "?")[0]
	if index := strings.IndexByte(path, '?'); index >= 0 {
		endpoint.RawQuery = path[index+1:]
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Origin", c.origin)
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	response, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, 2*1024*1024)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var value struct {
			Error struct{ Code, Message string } `json:"error"`
		}
		_ = json.NewDecoder(limited).Decode(&value)
		if value.Error.Message == "" {
			value.Error.Message = response.Status
		}
		return &APIError{Status: response.StatusCode, Code: value.Error.Code, Message: value.Error.Message}
	}
	if output == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(limited).Decode(output); err != nil {
		return fmt.Errorf("decode server response: %w", err)
	}
	return nil
}
