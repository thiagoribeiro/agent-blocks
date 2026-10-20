package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5"
	"github.com/thiagoribeiro/agent-blocks/internal/httpx"
	"github.com/thiagoribeiro/agent-blocks/internal/plugin"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"github.com/thiagoribeiro/agent-blocks/internal/roomadmin"
)

func failure(w http.ResponseWriter, err error) {
	code := 500
	var adminErr *roomadmin.Error
	switch {
	case errors.Is(err, plugin.ErrInvalid):
		code = 400
	case errors.Is(err, pgx.ErrNoRows):
		code = 404
	case errors.Is(err, plugin.ErrForbidden):
		code = 403
	case errors.Is(err, plugin.ErrConflict):
		code = 409
	case errors.As(err, &adminErr):
		code = adminErr.Status
	}
	http.Error(w, "operation failed", code)
}
func main() {
	if len(os.Args) > 1 && os.Args[1] == "rewind-replay" {
		if err := rewindReplay(context.Background(), os.Args[2:]); err != nil {
			slog.Error("rewind-replay failed", "error", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "rehash-journal" {
		if err := rehashJournal(context.Background()); err != nil {
			slog.Error("rehash-journal failed", "error", err)
			os.Exit(1)
		}
		return
	}
	token := os.Getenv("PLUGIN_TOKEN")
	name := os.Getenv("BRIAN_PLUGIN")
	dsn := os.Getenv("PLUGIN_DB_DSN")
	public := os.Getenv("PLUGIN_PUBLIC_URL")
	router := os.Getenv("AGENT_BLOCKS_URL")
	trama := os.Getenv("TRAMA_URL")
	if token == "" || name == "" || dsn == "" || public == "" || router == "" || trama == "" {
		slog.Error("plugin config required: BRIAN_PLUGIN, PLUGIN_TOKEN, PLUGIN_DB_DSN, PLUGIN_PUBLIC_URL, AGENT_BLOCKS_URL, TRAMA_URL")
		os.Exit(1)
	}
	ctx := context.Background()
	store, err := plugin.Open(ctx, dsn, name)
	if err != nil {
		slog.Error("plugin database unavailable", "error", err)
		os.Exit(1)
	}
	defer store.Pool.Close()
	// An opaque next_batch issued to this consumer account; empty starts from the beginning.
	if err = store.BootstrapCursor(ctx, "sync", os.Getenv("PLUGIN_START_CURSOR")); err != nil {
		slog.Error("cursor initialization failed")
		os.Exit(1)
	}
	runtime := &plugin.Runtime{Store: store, Wake: make(chan struct{}, 1), Admissions: make(chan struct{}, 1), Client: &http.Client{Timeout: 20 * time.Second}, Token: token, PublicURL: public, RouterURL: router, TramaURL: trama, NimletURL: httpx.Env("NIMLET_URL", "http://nimlet:8080"), NimletToken: os.Getenv("NIMLET_CONSUMER_TOKEN"), RiteSmithURL: os.Getenv("RITESMITH_URL")}
	if runtime.NimletToken == "" {
		slog.Warn("NIMLET_CONSUMER_TOKEN not set: sync, history replay and delegated commands are disabled")
	}
	runtime.SharingEnabled = os.Getenv("BRIAN_SHARING_ENABLED") == "true"
	if runtime.SharingEnabled {
		if os.Getenv("ROOM_ADMIN_URL") == "" || os.Getenv("ROOM_ADMIN_TOKEN") == "" {
			slog.Error("room admin configuration required")
			os.Exit(1)
		}
		runtime.Admin = &roomadmin.Client{URL: os.Getenv("ROOM_ADMIN_URL"), Token: os.Getenv("ROOM_ADMIN_TOKEN"), HTTP: &http.Client{Timeout: 35 * time.Second}}
	}
	if runtime.Admin == nil && os.Getenv("ROOM_ADMIN_URL") != "" && os.Getenv("ROOM_ADMIN_TOKEN") != "" {
		runtime.Admin = &roomadmin.Client{URL: os.Getenv("ROOM_ADMIN_URL"), Token: os.Getenv("ROOM_ADMIN_TOKEN"), HTTP: &http.Client{Timeout: 35 * time.Second}}
	}
	runtime.SurfacePublishers = map[string]bool{}
	for _, id := range strings.Split(os.Getenv("BRIAN_PUBLISHER_IDS"), ",") {
		if id = strings.TrimSpace(id); id != "" {
			runtime.SurfacePublishers[id] = true
		}
	}
	if runtime.SharingEnabled && len(runtime.SurfacePublishers) == 0 {
		slog.Error("BRIAN_PUBLISHER_IDS required for trusted source provenance")
		os.Exit(1)
	}
	runtime.DelegatingAgents = map[string]bool{}
	for _, id := range strings.Split(os.Getenv("BRIAN_DELEGATING_AGENT_IDS"), ",") {
		if id = strings.TrimSpace(id); id != "" {
			runtime.DelegatingAgents[id] = true
		}
	}
	if name == "agent_status" {
		go runtime.RunStatus(ctx)
	} else {
		workers, err := strconv.Atoi(httpx.Env("PLUGIN_ADMISSION_WORKERS", "1"))
		if err != nil || workers < 1 {
			slog.Error("PLUGIN_ADMISSION_WORKERS must be a positive integer")
			os.Exit(1)
		}
		runtime.RunAdmissions(ctx, workers)
		go runtime.Run(ctx)
	}
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		if err := store.Pool.Ping(r.Context()); err != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		httpx.WriteJSON(w, 200, map[string]string{"status": "ok"})
	})
	// Completion and failure are called by the workflow engine with the
	// per-operation token (or, for older executions, the plugin token).
	operationAuth := func(r *http.Request, id string) bool {
		return httpx.Authorized(r, token) || httpx.Authorized(r, protocol.OperationToken(token, id))
	}
	r.Post("/operations/{id}/complete", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if !operationAuth(r, id) {
			http.Error(w, "unauthorized", 401)
			return
		}
		if err := store.Complete(r.Context(), id); err != nil {
			failure(w, err)
			return
		}
		if created, admitted, err := store.PublicationTimes(r.Context(), id); err == nil {
			attrs := []any{"plugin", name, "operation", id, "total_ms", time.Since(created).Milliseconds()}
			if admitted != nil {
				attrs = append(attrs, "admit_ms", admitted.Sub(created).Milliseconds())
			}
			slog.Info("operation published", attrs...)
		}
		httpx.WriteJSON(w, 200, map[string]bool{"ok": true})
	})
	r.Post("/operations/{id}/failed", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if !operationAuth(r, id) {
			http.Error(w, "unauthorized", 401)
			return
		}
		if err := runtime.PublicationFailed(r.Context(), id); err != nil {
			failure(w, err)
			return
		}
		httpx.WriteJSON(w, 200, map[string]bool{"ok": true})
	})
	r.Group(func(r chi.Router) {
		r.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !httpx.Authorized(r, token) {
					http.Error(w, "unauthorized", 401)
					return
				}
				next.ServeHTTP(w, r)
			})
		})
		r.Post("/instances", func(w http.ResponseWriter, r *http.Request) {
			var req protocol.Create
			if !httpx.Decode(w, r, &req) {
				return
			}
			if name == "agent_status" {
				if err := runtime.CheckStatusAccess(r.Context(), req.RoomID, req.OwnerID); err != nil {
					failure(w, err)
					return
				}
			}
			i, err := store.Create(r.Context(), req)
			if err != nil {
				failure(w, err)
				return
			}
			runtime.Notify()
			httpx.WriteJSON(w, 201, i)
		})
		r.Post("/status-publication/resume", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				RoomID  string `json:"room_id"`
				OwnerID string `json:"owner_id"`
			}
			if !httpx.Decode(w, r, &req) {
				return
			}
			if name != "agent_status" {
				http.NotFound(w, r)
				return
			}
			if err := runtime.CheckStatusAccess(r.Context(), req.RoomID, req.OwnerID); err != nil {
				failure(w, err)
				return
			}
			if err := store.ResumeStatus(r.Context(), req.RoomID); err != nil {
				failure(w, err)
				return
			}
			runtime.Notify()
			httpx.WriteJSON(w, 200, map[string]bool{"ok": true})
		})
		r.Post("/instances/{id}/updates", func(w http.ResponseWriter, r *http.Request) {
			var update plugin.StatusUpdate
			if !httpx.Decode(w, r, &update) {
				return
			}
			instance, err := runtime.UpdateStatus(r.Context(), chi.URLParam(r, "id"), update)
			if err != nil {
				failure(w, err)
				return
			}
			httpx.WriteJSON(w, 200, instance)
		})
		r.Get("/instances/{id}", func(w http.ResponseWriter, r *http.Request) {
			if actor := r.URL.Query().Get("actor"); actor != "" {
				i, err := store.Get(r.Context(), chi.URLParam(r, "id"))
				if err != nil {
					failure(w, err)
					return
				}
				if r.URL.Query().Get("room_id") != i.RoomID || !plugin.CanEdit(i.State, i.OwnerID, actor) {
					failure(w, plugin.ErrForbidden)
					return
				}
				if i.State["shared"] != nil {
					if runtime.Admin == nil {
						http.Error(w, "membership service unavailable", 503)
						return
					}
					members, err := runtime.Admin.Do(r.Context(), roomadmin.Request{Action: "members", RoomID: i.RoomID, Actor: actor})
					if err != nil {
						http.Error(w, "membership unavailable", 503)
						return
					}
					if err = store.SyncMembers(r.Context(), i.ID, members.Members); err != nil {
						failure(w, err)
						return
					}
				}
			}

			if err := store.CurrentBills(r.Context(), chi.URLParam(r, "id")); err != nil {
				failure(w, err)
				return
			}
			i, err := store.Get(r.Context(), chi.URLParam(r, "id"))
			if err != nil {
				failure(w, err)
				return
			}
			httpx.WriteJSON(w, 200, i)
		})
		r.Post("/events", func(w http.ResponseWriter, r *http.Request) {
			var e protocol.Event
			if !httpx.Decode(w, r, &e) {
				return
			}
			op, err := runtime.Handle(r.Context(), e)
			if err != nil {
				failure(w, err)
				return
			}
			runtime.Notify()
			httpx.WriteJSON(w, 202, map[string]any{"accepted": true, "operation_id": op.ID})
		})
		r.Get("/operations/{id}", func(w http.ResponseWriter, r *http.Request) {
			op, err := store.Operation(r.Context(), chi.URLParam(r, "id"))
			if err != nil {
				failure(w, err)
				return
			}
			httpx.WriteJSON(w, 200, op)
		})
	})
	srv := http.Server{Addr: httpx.Env("PLUGIN_HTTP_ADDR", ":4010"), Handler: r, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second}
	slog.Info("plugin starting", "plugin", name)
	if err = srv.ListenAndServe(); err != nil {
		slog.Error("plugin stopped", "error", err)
		os.Exit(1)
	}
}
