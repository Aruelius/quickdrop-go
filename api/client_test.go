package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientBuildsRequestsAndWebSocketHeaders(t *testing.T) {
	var authorization, origin string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization, origin = r.Header.Get("Authorization"), r.Header.Get("Origin")
		if r.URL.Path != "/base/api/sessions/join" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		var input map[string]string
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input["code"] != "123456" {
			t.Fatalf("input=%v err=%v", input, err)
		}
		_ = json.NewEncoder(w).Encode(Credentials{SessionID: "session", PeerID: "peer", PeerToken: "secret"})
	}))
	defer server.Close()
	client, err := New(server.URL+"/base/", "qdv1_test-secret-value-12345678901234567890")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.JoinSession(context.Background(), " 123456 "); err != nil {
		t.Fatal(err)
	}
	if authorization != "Bearer "+client.Token() || origin != server.URL {
		t.Fatalf("authorization=%q origin=%q", authorization, origin)
	}
	header := client.WebSocketHeader()
	if header.Get("Origin") != server.URL || header.Get("Authorization") != authorization {
		t.Fatalf("websocket headers=%v", header)
	}
}

func TestClientReturnsStructuredAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "denied", "message": "no access"}})
	}))
	defer server.Close()
	client, _ := New(server.URL, "")
	_, err := client.CreateSession(context.Background())
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Status != http.StatusUnauthorized || apiErr.Code != "denied" {
		t.Fatalf("error=%#v", err)
	}
}

func TestClientRejectsRemotePlainHTTPByDefault(t *testing.T) {
	if _, err := New("http://drop.example.test", ""); err == nil {
		t.Fatal("remote plain HTTP server was accepted")
	}
	if _, err := NewWithOptions("http://drop.example.test", "", ClientOptions{AllowInsecureHTTP: true}); err != nil {
		t.Fatalf("explicit insecure opt-out failed: %v", err)
	}
	for _, endpoint := range []string{"http://localhost:8080", "http://127.0.0.1:8080", "http://192.168.1.10:8080"} {
		if _, err := New(endpoint, ""); err != nil {
			t.Fatalf("local endpoint %s rejected: %v", endpoint, err)
		}
	}
}

func TestLoginCookieIsUsedToCreateDeviceToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/login":
			http.SetCookie(w, &http.Cookie{Name: "quickdrop_session", Value: "session-secret", Path: "/", HttpOnly: true})
			_ = json.NewEncoder(w).Encode(AuthState{User: User{ID: "user-1", Username: "alice"}})
		case "/api/account/device-tokens":
			cookie, err := r.Cookie("quickdrop_session")
			if err != nil || cookie.Value != "session-secret" {
				t.Fatalf("login cookie missing: cookie=%v err=%v", cookie, err)
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(DeviceToken{ID: "token-1", Name: "server", Token: "qdv1_secret"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, _ := New(server.URL, "")
	if _, err := client.Login(context.Background(), "alice", "password"); err != nil {
		t.Fatal(err)
	}
	token, err := client.CreateDeviceToken(context.Background(), "server")
	if err != nil || token.Token != "qdv1_secret" {
		t.Fatalf("token=%+v err=%v", token, err)
	}
}
