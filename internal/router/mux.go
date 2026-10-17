// Package router is the Agent Blocks AppService router: it receives Nimlet
// events, forwards plugin commands to their plugin services, and publishes the
// plugins' authoritative results back to Nimlet.
package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/thiagoribeiro/agent-blocks/internal/httpx"
)

// NewMux returns the router's HTTP handler.
func NewMux(cfg Config) http.Handler {
	h := newAppServiceHandler(cfg)
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.Recoverer)
	r.Post("/events", h.handleEvents)
	r.Put("/events", h.handleEvents)
	r.Put("/transactions/{txnId}", h.handleEvents)
	r.Get("/catalog", h.handleCatalog)
	r.Post("/plugins/{plugin}/instances", h.proxy)
	r.Get("/plugins/{plugin}/instances/{instance}", h.proxy)
	r.Post("/plugins/{plugin}/instances/{instance}/updates", h.statusUpdate)
	r.Get("/plugins/{plugin}/operations/{operation}", h.operation)
	r.Post("/plugins/{plugin}/operations/{operation}/publish", h.publish)
	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return r
}
