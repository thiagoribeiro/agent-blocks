package plugin

import (
	"context"
	"errors"
	"fmt"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestStatusContractRejectsMalformedSnapshots(t *testing.T) {
	started := time.Now().UTC().Format(time.RFC3339Nano)
	state, err := initialStatus(protocol.Create{Title: "Activity", OwnerID: "owner", Data: map[string]any{"request_id": "r", "started_at": started}})
	if err != nil || validateStatus(state) != nil {
		t.Fatal(state, err)
	}
	for _, key := range []string{"request_id", "heartbeat_at", "history_total", "history"} {
		v := clone(state)
		delete(v, key)
		if validateStatus(v) == nil {
			t.Fatal("accepted missing field", key)
		}
	}
	v := clone(state)
	v["arguments"] = map[string]any{"secret": "x"}
	if validateStatus(v) == nil {
		t.Fatal("accepted extra data")
	}
	if _, err = Apply("agent_status", state, "owner", "owner", "update", map[string]any{}, time.Now()); !errors.Is(err, ErrForbidden) {
		t.Fatal("human can modify activity", err)
	}
}
func TestPostgresStatusProducerReplayAndDirectPublication(t *testing.T) {
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("BRIAN_TEST_DSN required")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, "agent_status")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	req := protocol.Create{Key: fmt.Sprint(time.Now().UnixNano()), Title: "Atividade do agente", RoomID: "room", OwnerID: "owner", Data: map[string]any{"request_id": "request", "started_at": time.Now().UTC().Format(time.RFC3339Nano)}}
	instance, err := s.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	r := &Runtime{Store: s, Wake: make(chan struct{}, 1), DelegatingAgents: map[string]bool{"agent": true}}
	state := clone(instance.State)
	state["sequence"] = float64(2)
	state["current_status"] = "Pensando"
	update := StatusUpdate{RoomID: "room", OwnerID: "owner", ProducerID: "agent", State: state}
	wrong := update
	wrong.ProducerID = "human"
	if _, err = r.UpdateStatus(ctx, instance.ID, wrong); !errors.Is(err, ErrForbidden) {
		t.Fatal("producer", err)
	}
	wrong = update
	wrong.RoomID = "other"
	if _, err = r.UpdateStatus(ctx, instance.ID, wrong); !errors.Is(err, ErrForbidden) {
		t.Fatal("room", err)
	}
	first, err := r.UpdateStatus(ctx, instance.ID, update)
	if err != nil {
		t.Fatal(err)
	}
	again, err := r.UpdateStatus(ctx, instance.ID, update)
	if err != nil || revision(first.State) != revision(again.State) {
		t.Fatal("duplicate changed revision", err)
	}
	conflict := update
	conflict.State = clone(state)
	conflict.State["current_status"] = "Different"
	if _, err = r.UpdateStatus(ctx, instance.ID, conflict); !errors.Is(err, ErrConflict) {
		t.Fatal("same sequence reused", err)
	}
	final := clone(state)
	final["sequence"] = float64(3)
	final["status"] = "completed"
	final["current_status"] = "Done"
	final["finished_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	update.State = final
	if _, err = r.UpdateStatus(ctx, instance.ID, update); err != nil {
		t.Fatal(err)
	}
	update.State = state
	after, err := r.UpdateStatus(ctx, instance.ID, update)
	if err != nil || str(after.State, "status") != "completed" {
		t.Fatal("old update regressed", err)
	}
	var publications int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer token" {
			t.Error("auth missing")
		}
		atomic.AddInt64(&publications, 1)
		w.WriteHeader(200)
	}))
	defer server.Close()
	r.Client = server.Client()
	r.RouterURL = server.URL
	r.Token = "token"
	runctx, cancel := context.WithCancel(ctx)
	go r.RunStatus(runctx)
	defer cancel()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		pending, err := s.Pending(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) == 0 {
			if atomic.LoadInt64(&publications) < 3 {
				t.Fatal("lost publication")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("pending journal not recovered")
}
