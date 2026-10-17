package plugin

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"github.com/thiagoribeiro/agent-blocks/internal/roomadmin"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestStatusAccessRequiresPublisherWithoutJoining(t *testing.T) {
	joined := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		var req roomadmin.Request
		if err := json.NewDecoder(q.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Action != "members" || req.RoomID != "room" || req.Actor != "owner" {
			t.Error(req)
		}
		members := []roomadmin.Member{{UserID: "owner", State: "join"}}
		if joined {
			members = append(members, roomadmin.Member{UserID: "publisher", State: "join"})
		}
		_ = json.NewEncoder(w).Encode(roomadmin.Response{Members: members})
	}))
	defer srv.Close()
	r := Runtime{Admin: &roomadmin.Client{URL: srv.URL, HTTP: srv.Client()}, SurfacePublishers: map[string]bool{"publisher": true}}
	if err := r.CheckStatusAccess(context.Background(), "room", "owner"); err != ErrForbidden {
		t.Fatal(err)
	}
	joined = true
	if err := r.CheckStatusAccess(context.Background(), "room", "owner"); err != nil {
		t.Fatal(err)
	}
}

func TestStatusDelayIsBoundedWhenPoolIsExhausted(t *testing.T) {
	s := statusPublicationStore(t)
	connections := make([]*pgxpool.Conn, 0, s.Pool.Config().MaxConns)
	defer func() {
		for _, conn := range connections {
			conn.Release()
		}
	}()
	for range s.Pool.Config().MaxConns {
		conn, err := s.Pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
	}
	finished := make(chan time.Duration, 1)
	go func() { finished <- s.StatusDelay(context.Background()) }()
	select {
	case delay := <-finished:
		if delay != time.Second {
			t.Fatal(delay)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle scheduling blocked indefinitely on the pool")
	}
}

func TestRetryStatusReturnsDurableAttemptCount(t *testing.T) {
	s := statusPublicationStore(t)
	i := statusPublicationInstance(t, s, "room")
	for expected := 1; expected <= 2; expected++ {
		count, err := s.RetryStatus(context.Background(), "create:"+i.ID)
		if err != nil || count != expected {
			t.Fatal(count, err)
		}
	}
}

func TestStatusSuspensionSurvivesRestartAndExplicitResume(t *testing.T) {
	s := statusPublicationStore(t)
	i := statusPublicationInstance(t, s, "blocked")
	r := Runtime{Store: s, Wake: make(chan struct{}, 1), DelegatingAgents: map[string]bool{"agent": true}}
	state := clone(i.State)
	state["sequence"] = float64(2)
	if _, err := r.UpdateStatus(context.Background(), i.ID, StatusUpdate{RoomID: i.RoomID, OwnerID: "owner", ProducerID: "agent", State: state}); err != nil {
		t.Fatal(err)
	}
	if err := s.SuspendStatus(context.Background(), i.RoomID, "publication returned 404"); err != nil {
		t.Fatal(err)
	}
	ids, err := s.StatusPending(context.Background())
	if err != nil || len(ids) != 0 {
		t.Fatal(ids, err)
	}
	var count int
	if err := s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM brian_agent_status.operations WHERE NOT done AND status_publish_suspended IS NOT NULL").Scan(&count); err != nil || count != 2 {
		t.Fatal(count, err)
	}
	if err := s.ResumeStatus(context.Background(), i.RoomID); err != nil {
		t.Fatal(err)
	}
	ids, err = s.StatusPending(context.Background())
	if err != nil || len(ids) != 1 || ids[0] != "create:"+i.ID {
		t.Fatal(ids, err)
	}
}

func TestStatusPermanentFailureStopsRetries(t *testing.T) {
	s := statusPublicationStore(t)
	i := statusPublicationInstance(t, s, "blocked")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) { w.WriteHeader(404) }))
	defer srv.Close()
	startStatusPublisher(t, s, srv)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var reason *string
		if err := s.Pool.QueryRow(context.Background(), "SELECT status_publish_suspended FROM brian_agent_status.operations WHERE id=$1", "create:"+i.ID).Scan(&reason); err != nil {
			t.Fatal(err)
		}
		if reason != nil {
			ids, err := s.StatusPending(context.Background())
			if err != nil || len(ids) != 0 {
				t.Fatal(ids, err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("404 was not suspended")
}

func TestReconciliationKeepsItsFrontierAndRetriesFailedPages(t *testing.T) {
	s := statusPublicationStore(t)
	i := statusPublicationInstance(t, s, "room")
	var starts []int64
	fail := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		from, _ := strconv.ParseInt(q.URL.Query().Get("from"), 10, 64)
		starts = append(starts, from)
		if fail {
			w.WriteHeader(503)
			return
		}
		chunk := []protocol.Event{}
		end := from
		if from < 12 {
			chunk = append(chunk, protocol.Event{Type: "m.room.message", RoomID: i.RoomID, RoomSeq: 12})
			end = 12
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"chunk": chunk, "end": end})
	}))
	defer srv.Close()
	r := Runtime{Store: s, Client: srv.Client(), NimletURL: srv.URL, NimletToken: "consumer"}
	ctx := context.Background()
	if err := r.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Cursor(ctx, "replay:room"); got != "12" {
		t.Fatal(got)
	}
	fail = true
	if err := r.Reconcile(ctx); err == nil {
		t.Fatal("expected failure")
	}
	fail = false
	if err := r.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(starts) != 3 || starts[1] != 12 || starts[2] != 12 {
		t.Fatal(starts)
	}
}

func TestReconciliationCommitsEachPageAndBoundsRecovery(t *testing.T) {
	s := statusPublicationStore(t)
	statusPublicationInstance(t, s, "room")
	failSecond := true
	var starts []int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		from, _ := strconv.Atoi(q.URL.Query().Get("from"))
		starts = append(starts, from)
		if from == 100 && failSecond {
			w.WriteHeader(503)
			return
		}
		chunk := make([]protocol.Event, 100)
		for i := range chunk {
			chunk[i] = protocol.Event{Type: "m.room.message", RoomSeq: int64(from + i + 1)}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"chunk": chunk, "end": from + 100})
	}))
	defer srv.Close()
	r := Runtime{Store: s, Client: srv.Client(), NimletURL: srv.URL, NimletToken: "consumer"}
	if err := r.Reconcile(context.Background()); err == nil {
		t.Fatal("expected second-page failure")
	}
	if cursor, _ := s.Cursor(context.Background(), "replay:room"); cursor != "100" {
		t.Fatal(cursor)
	}
	failSecond = false
	starts = nil
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(starts) != 5 || starts[0] != 100 || starts[4] != 500 {
		t.Fatal(starts)
	}
	if cursor, _ := s.Cursor(context.Background(), "replay:room"); cursor != "600" {
		t.Fatal(cursor)
	}
}
