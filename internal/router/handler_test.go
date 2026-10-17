package router

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/thiagoribeiro/agent-blocks/internal/httpx"
	"github.com/thiagoribeiro/agent-blocks/internal/nimlet"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
)

type fakePlugin struct {
	status int
	calls  int
}

func TestStatusUpdatesRequireInternalAuthorityAndOwnPlugin(t *testing.T) {
	h := newAppServiceHandler(Config{InternalToken: "runtime-secret"})
	p := &fakePlugin{status: 200}
	h.registry.handlers["agent_status"] = p
	r := chi.NewRouter()
	r.Post("/plugins/{plugin}/instances/{instance}/updates", h.statusUpdate)
	for _, tc := range []struct {
		token, plugin string
		code, calls   int
	}{
		{"user-token", "agent_status", 401, 0},
		{"runtime-secret", "workflow", 404, 0},
		{"runtime-secret", "agent_status", 200, 1},
	} {
		q := httptest.NewRequest("POST", "/plugins/"+tc.plugin+"/instances/card/updates", bytes.NewBufferString(`{"room_id":"room","owner_id":"owner","producer_id":"agent","state":{}}`))
		q.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, q)
		if w.Code != tc.code || p.calls != tc.calls {
			t.Fatal(w.Code, p.calls)
		}
	}
}

func (p *fakePlugin) Request(context.Context, string, string, []byte) (int, []byte, error) {
	p.calls++
	return p.status, []byte(`{}`), nil
}
func TestWebhookAcknowledgesDurablePluginOnly(t *testing.T) {
	h := newAppServiceHandler(Config{HsToken: "nano-token"})
	p := &fakePlugin{status: 503}
	h.registry.handlers["approval"] = p
	content, _ := json.Marshal(protocol.Command{Plugin: "approval", InstanceID: "instance", Command: "approve", Payload: map[string]any{}})
	b, _ := json.Marshal(EventBatch{Events: []protocol.Event{{Type: "m.plugin.command", EventID: "event", RoomID: "room", SenderID: "owner", Content: content}}})
	run := func(token string) int {
		r := httptest.NewRequest("POST", "/events", bytes.NewReader(b))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.handleEvents(w, r)
		return w.Code
	}
	if code := run("wrong"); code != 401 || p.calls != 0 {
		t.Fatal(code, p.calls)
	}
	if code := run("nano-token"); code != 503 {
		t.Fatal(code)
	}
	p.status = 202
	if code := run("nano-token"); code != 200 {
		t.Fatal(code)
	}
}
func TestPublicationFetchesAuthorityAndStableTransaction(t *testing.T) {
	var paths []string
	nano := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer as-token" {
			t.Error("wrong appservice token")
		}
		httpx.WriteJSON(w, 200, nimlet.SendEventResponse{EventID: "result", RoomSeq: 10})
	}))
	defer nano.Close()
	plugin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.Header.Get("Authorization") != "Bearer plugin-token" {
			t.Error("wrong boundary")
		}
		httpx.WriteJSON(w, 200, protocol.Operation{ID: "event", RoomID: "room", EventType: "m.plugin.state.updated", Content: map[string]any{"view": map[string]any{"status": "approved"}}})
	}))
	defer plugin.Close()
	h := newAppServiceHandler(Config{NimletURL: nano.URL, AsToken: "as-token", Plugins: []PluginConfig{{Name: "approval", URL: plugin.URL, Token: "plugin-token"}}})
	r := chi.NewRouter()
	r.Post("/plugins/{plugin}/operations/{operation}/publish", h.publish)
	for _, token := range []string{"wrong", "plugin-token", "plugin-token"} {
		req := httptest.NewRequest("POST", "/plugins/approval/operations/event/publish", bytes.NewBufferString(`{"view":{"status":"evil"}}`))
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		expected := 200
		if token == "wrong" {
			expected = 401
		}
		if w.Code != expected {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if len(paths) != 2 || paths[0] != paths[1] {
		t.Fatal(paths)
	}
}

func TestOperationReadRequiresInternalToken(t *testing.T) {
	h := newAppServiceHandler(Config{InternalToken: "internal"})
	p := &fakePlugin{status: 200}
	h.registry.handlers["checklist"] = p
	r := chi.NewRouter()
	r.Get("/plugins/{plugin}/operations/{operation}", h.operation)
	for _, token := range []string{"wrong", "internal"} {
		q := httptest.NewRequest("GET", "/plugins/checklist/operations/event", nil)
		q.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, q)
		expected := 200
		if token == "wrong" {
			expected = 401
		}
		if w.Code != expected {
			t.Fatal(w.Code)
		}
	}
	if p.calls != 1 {
		t.Fatal("unauthorized read reached plugin")
	}
}
