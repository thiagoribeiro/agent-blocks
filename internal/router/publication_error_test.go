package router

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/thiagoribeiro/agent-blocks/internal/httpx"
	"github.com/thiagoribeiro/agent-blocks/internal/nimlet"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
)

func TestPublicationPreservesUpstreamFailure(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 408, 429, 500, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			nano := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte("private upstream details"))
			}))
			defer nano.Close()
			plugin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				httpx.WriteJSON(w, 200, protocol.Operation{ID: "event", RoomID: "room", EventType: "m.ui.message"})
			}))
			defer plugin.Close()
			h := newAppServiceHandler(Config{NimletURL: nano.URL, AsToken: "secret", Plugins: []PluginConfig{{Name: "agent_status", URL: plugin.URL, Token: "plugin"}}})
			router := chi.NewRouter()
			router.Post("/plugins/{plugin}/operations/{operation}/publish", h.publish)
			r := httptest.NewRequest("POST", "/plugins/agent_status/operations/event/publish", nil)
			r.Header.Set("Authorization", "Bearer plugin")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			var failure protocol.HTTPError
			if err := json.Unmarshal(w.Body.Bytes(), &failure); err != nil {
				t.Fatal(err)
			}
			if w.Code != status || failure.Status != status || failure.Retryable != (status >= 500 || status == 408 || status == 429) {
				t.Fatal(w.Code, failure)
			}
		})
	}
}

func TestPublicationForwardsAuthoritativeJSONWithoutChangingNumbers(t *testing.T) {
	content := json.RawMessage(`{ "sequence":9007199254740993, "view":{"label":"queijo","checked":false} }`)
	var received []byte
	nano := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		received, err = io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		httpx.WriteJSON(w, 200, nimlet.SendEventResponse{EventID: "published", RoomSeq: 1})
	}))
	defer nano.Close()
	plugin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Write the authoritative JSON directly: encoding a RawMessage would
		// normalize whitespace before this test's forwarding boundary.
		_, _ = w.Write(append(append([]byte(`{"operation_id":"event","room_id":"room","event_type":"m.ui.message","content":`), content...), '}'))
	}))
	defer plugin.Close()
	h := newAppServiceHandler(Config{NimletURL: nano.URL, AsToken: "secret", Plugins: []PluginConfig{{Name: "agent_status", URL: plugin.URL, Token: "plugin"}}})
	router := chi.NewRouter()
	router.Post("/plugins/{plugin}/operations/{operation}/publish", h.publish)
	r := httptest.NewRequest("POST", "/plugins/agent_status/operations/event/publish", strings.NewReader(`{"content":"untrusted replacement"}`))
	r.Header.Set("Authorization", "Bearer plugin")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	if w.Code != 200 || !bytes.Equal(received, content) {
		t.Fatalf("status=%d content=%s", w.Code, received)
	}
}

func TestSendEventRejectsMissingContentBeforeNetwork(t *testing.T) {
	c := nimlet.New("http://unused.invalid", "secret")
	if _, err := c.SendEvent(context.Background(), "app", "room", "m.ui.message", "txn", nil); err == nil {
		t.Fatal("missing content accepted")
	}
}

var publicationBenchmarkBody []byte

// Compare the old decode/marshal path with the opaque publication DTO. This is
// a local allocation/CPU benchmark, not a measurement of the board or HTTP I/O.
func BenchmarkPublicationJSON(b *testing.B) {
	history := make([]map[string]any, 200)
	for i := range history {
		history[i] = map[string]any{"id": i, "label": strings.Repeat("x", 120), "status": "completed"}
	}
	state := map[string]any{"sequence": 200, "history": history}
	data, err := json.Marshal(protocol.Operation{ID: "op", RoomID: "room", EventType: "m.plugin.state.updated", Content: map[string]any{"view": state, "state": state}})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("decoded-state-roundtrip", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			var op protocol.Operation
			if err := json.Unmarshal(data, &op); err != nil {
				b.Fatal(err)
			}
			publicationBenchmarkBody, err = json.Marshal(op.Content)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("opaque-json", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			var op publication
			if err := json.Unmarshal(data, &op); err != nil {
				b.Fatal(err)
			}
			publicationBenchmarkBody = op.Content
		}
	})
}

func TestPublicationAcceptsOnlyItsOperationToken(t *testing.T) {
	nimlet := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, 200, nimlet.SendEventResponse{EventID: "published", RoomSeq: 1})
	}))
	defer nimlet.Close()
	plugin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, 200, protocol.Operation{ID: "event", RoomID: "room", EventType: "m.ui.message", Content: map[string]any{"body": "ok"}})
	}))
	defer plugin.Close()
	h := newAppServiceHandler(Config{NimletURL: nimlet.URL, AsToken: "secret", Plugins: []PluginConfig{{Name: "agent_status", URL: plugin.URL, Token: "plugin"}}})
	router := chi.NewRouter()
	router.Post("/plugins/{plugin}/operations/{operation}/publish", h.publish)
	for token, want := range map[string]int{
		"plugin": 200,
		protocol.OperationToken("plugin", "event"): 200,
		protocol.OperationToken("plugin", "other"): 401,
		protocol.OperationToken("wrong", "event"):  401,
		"": 401,
	} {
		r := httptest.NewRequest("POST", "/plugins/agent_status/operations/event/publish", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != want {
			t.Errorf("token %q: status %d, want %d", token, w.Code, want)
		}
	}
}
