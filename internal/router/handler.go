package router

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"
	"github.com/thiagoribeiro/agent-blocks/internal/catalog"
	"github.com/thiagoribeiro/agent-blocks/internal/httpx"
	"github.com/thiagoribeiro/agent-blocks/internal/nimlet"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
)

type EventBatch struct {
	TransactionID string           `json:"transaction_id"`
	Events        []protocol.Event `json:"events"`
}

// Publication only routes plugin-owned JSON; decoding its domain state would
// allocate a second object tree and could round integers through float64.
type publication struct {
	ID        string          `json:"operation_id"`
	RoomID    string          `json:"room_id"`
	EventType string          `json:"event_type"`
	Content   json.RawMessage `json:"content"`
}
type appServiceHandler struct {
	cfg      Config
	registry *Registry
	nc       *nimlet.Client
}

func newAppServiceHandler(cfg Config) *appServiceHandler {
	r := newRegistry()
	for _, p := range cfg.Plugins {
		r.Register(p)
	}
	return &appServiceHandler{cfg: cfg, registry: r, nc: nimlet.New(cfg.NimletURL, cfg.AsToken)}
}
func (h *appServiceHandler) handleEvents(w http.ResponseWriter, r *http.Request) {
	if !httpx.Authorized(r, h.cfg.HsToken) {
		http.Error(w, "unauthorized", 401)
		return
	}
	var batch EventBatch
	// Transport may add fields; plugin command validation is stricter at the boundary.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if json.NewDecoder(r.Body).Decode(&batch) != nil || len(batch.Events) > 1000 {
		http.Error(w, "invalid batch", 400)
		return
	}
	for _, e := range batch.Events {
		if e.Type != "m.plugin.command" {
			continue
		}
		var c protocol.Command
		if json.Unmarshal(e.Content, &c) != nil || c.Plugin == "" {
			http.Error(w, "invalid command", 400)
			return
		}
		p, ok := h.registry.Get(c.Plugin)
		if !ok {
			slog.Warn("unknown plugin command", "event_id", e.EventID, "plugin", c.Plugin)
			continue
		}
		b, _ := json.Marshal(e)
		code, _, err := p.Request(r.Context(), "POST", "/events", b)
		if err != nil || code >= 500 {
			http.Error(w, "plugin unavailable", 503)
			return
		}
		if code != 202 && code != 200 {
			http.Error(w, fmt.Sprintf("plugin rejected envelope: %d", code), 400)
			return
		}
	}
	httpx.WriteJSON(w, 200, map[string]any{"accepted": true})
}
func (h *appServiceHandler) handleCatalog(w http.ResponseWriter, r *http.Request) {
	if !httpx.Authorized(r, h.cfg.InternalToken) {
		http.Error(w, "unauthorized", 401)
		return
	}
	c, err := catalog.Runtime(h.registry.Names())
	if err != nil {
		http.Error(w, "catalog error", 500)
		return
	}
	httpx.WriteJSON(w, 200, c)
}
func (h *appServiceHandler) proxy(w http.ResponseWriter, r *http.Request) {
	if !httpx.Authorized(r, h.cfg.InternalToken) {
		http.Error(w, "unauthorized", 401)
		return
	}
	name := chi.URLParam(r, "plugin")
	p, ok := h.registry.Get(name)
	if !ok {
		http.Error(w, "unknown plugin", 404)
		return
	}
	path := "/instances"
	if id := chi.URLParam(r, "instance"); id != "" {
		path += "/" + url.PathEscape(id)
	}
	var body []byte
	if r.Method == "POST" {
		var raw json.RawMessage
		if !httpx.Decode(w, r, &raw) {
			return
		}
		body = raw
	}
	if r.Method == "GET" {
		q := url.Values{}
		for _, k := range []string{"actor", "room_id"} {
			if v := r.URL.Query().Get(k); v != "" {
				q.Set(k, v)
			}
		}
		if encoded := q.Encode(); encoded != "" {
			path += "?" + encoded
		}
	}
	code, b, err := p.Request(r.Context(), r.Method, path, body)
	if err != nil {
		http.Error(w, "plugin unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(b)
}

// operation exposes only the plugin-owned journal to authenticated internal readers.
func (h *appServiceHandler) operation(w http.ResponseWriter, r *http.Request) {
	if !httpx.Authorized(r, h.cfg.InternalToken) {
		http.Error(w, "unauthorized", 401)
		return
	}
	p, ok := h.registry.Get(chi.URLParam(r, "plugin"))
	if !ok {
		http.Error(w, "unknown plugin", 404)
		return
	}
	code, body, err := p.Request(r.Context(), "GET", "/operations/"+url.PathEscape(chi.URLParam(r, "operation")), nil)
	if err != nil {
		http.Error(w, "plugin unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

func (h *appServiceHandler) publish(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "plugin")
	cfg, ok := h.registry.configs[name]
	if !ok {
		http.Error(w, "unknown plugin", 404)
		return
	}
	id := chi.URLParam(r, "operation")
	// Workflow definitions carry a per-operation token; the plugin token is still
	// accepted for executions created before operation tokens existed.
	if !httpx.Authorized(r, cfg.Token) && !httpx.Authorized(r, protocol.OperationToken(cfg.Token, id)) {
		http.Error(w, "unauthorized", 401)
		return
	}
	var op publication
	if err := h.registry.Fetch(r.Context(), name, "/operations/"+url.PathEscape(id), &op); err != nil {
		http.Error(w, "operation unavailable", 503)
		return
	}
	// Fetch authoritative content from the owning plugin, never accept client-provided state.
	txn := "plugin:" + name + ":" + op.ID
	response, err := h.nc.SendEvent(r.Context(), "agent-blocks", op.RoomID, op.EventType, txn, op.Content)
	if err != nil {
		var upstream *protocol.HTTPError
		if errors.As(err, &upstream) {
			httpx.WriteJSON(w, upstream.Status, upstream)
		} else {
			httpx.WriteJSON(w, 503, &protocol.HTTPError{Status: 503, Code: "publication_unavailable", Retryable: true})
		}
		return
	}
	httpx.WriteJSON(w, 200, response)
}

// Progress updates are internal and can only target the runtime-owned status plugin.
func (h *appServiceHandler) statusUpdate(w http.ResponseWriter, r *http.Request) {
	if !httpx.Authorized(r, h.cfg.InternalToken) {
		http.Error(w, "unauthorized", 401)
		return
	}
	if chi.URLParam(r, "plugin") != "agent_status" {
		http.Error(w, "unsupported", 404)
		return
	}
	p, ok := h.registry.Get("agent_status")
	if !ok {
		http.Error(w, "unavailable", 404)
		return
	}
	var raw json.RawMessage
	if !httpx.Decode(w, r, &raw) {
		return
	}
	code, b, err := p.Request(r.Context(), "POST", "/instances/"+url.PathEscape(chi.URLParam(r, "instance"))+"/updates", raw)
	if err != nil {
		http.Error(w, "unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(b)
}
