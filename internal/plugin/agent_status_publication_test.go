package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
)

func statusPublicationStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("BRIAN_TEST_DSN required")
	}
	s, err := Open(context.Background(), dsn, "agent_status")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Pool.Close)
	t.Cleanup(func() {
		_, _ = s.Pool.Exec(context.Background(), "TRUNCATE brian_agent_status.instances,brian_agent_status.operations,brian_agent_status.cursors")
	})
	if _, err = s.Pool.Exec(context.Background(), "TRUNCATE brian_agent_status.instances,brian_agent_status.operations,brian_agent_status.cursors"); err != nil {
		t.Fatal(err)
	}
	return s
}
func statusPublicationInstance(t *testing.T, s *Store, room string) *protocol.Instance {
	t.Helper()
	i, err := s.Create(context.Background(), protocol.Create{
		Key: room + fmt.Sprint(time.Now().UnixNano()), Title: "Atividade", RoomID: room, OwnerID: "owner",
		Data: map[string]any{"request_id": room, "started_at": time.Now().UTC().Format(time.RFC3339Nano)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return i
}
func startStatusPublisher(t *testing.T, s *Store, server *httptest.Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	r := &Runtime{Store: s, Client: server.Client(), RouterURL: server.URL, Token: "plugin-secret", Wake: make(chan struct{}, 1)}
	go func() { defer close(finished); r.RunStatus(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("publisher did not stop")
		}
	})
}
func operationID(r *http.Request) string {
	return strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/plugins/agent_status/operations/"), "/publish")
}
func TestStatusPublicationFailureDoesNotBlockOtherRooms(t *testing.T) {
	s := statusPublicationStore(t)
	bad := statusPublicationInstance(t, s, "publisher-not-member")
	good := statusPublicationInstance(t, s, "joined-room")
	var mu sync.Mutex
	published := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer plugin-secret" {
			t.Error("missing publication authority")
		}
		op, err := s.Operation(r.Context(), operationID(r))
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		if op.RoomID == bad.RoomID {
			w.WriteHeader(403)
			return
		}
		mu.Lock()
		published = append(published, op.InstanceID)
		mu.Unlock()
		w.WriteHeader(200)
	}))
	t.Cleanup(server.Close)
	startStatusPublisher(t, s, server)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ready := len(published) > 0
		mu.Unlock()
		if ready {
			if published[0] != good.ID {
				t.Fatal("wrong instance published")
			}
			pending, err := s.Pending(context.Background())
			if err != nil || len(pending) == 0 {
				t.Fatal("failed publication was lost", pending, err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("one room without publisher membership blocked an unrelated joined room")
}

func TestStatusRetrySurvivesRestartAndPreservesInstanceOrder(t *testing.T) {
	s := statusPublicationStore(t)
	bad := statusPublicationInstance(t, s, "blocked")
	good := statusPublicationInstance(t, s, "healthy")
	r := &Runtime{Store: s, Wake: make(chan struct{}, 1), DelegatingAgents: map[string]bool{"agent": true}}
	state := clone(bad.State)
	state["sequence"] = float64(2)
	if _, err := r.UpdateStatus(context.Background(), bad.ID, StatusUpdate{RoomID: bad.RoomID, OwnerID: "owner", ProducerID: "agent", State: state}); err != nil {
		t.Fatal(err)
	}
	createID := "create:" + bad.ID
	before, err := s.Operation(context.Background(), createID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.RetryStatus(context.Background(), createID); err != nil {
		t.Fatal(err)
	}
	var failures int
	var scheduled bool
	if err = s.Pool.QueryRow(context.Background(), "SELECT status_publish_failures,status_publish_after>clock_timestamp() FROM brian_agent_status.operations WHERE id=$1", createID).Scan(&failures, &scheduled); err != nil || failures != 1 || !scheduled {
		t.Fatal(failures, scheduled, err)
	}
	// Make the cooldown deterministic while checking migration/restart recovery.
	if _, err = s.Pool.Exec(context.Background(), "UPDATE brian_agent_status.operations SET status_publish_after=clock_timestamp()+interval '1 hour' WHERE id=$1", createID); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(context.Background(), os.Getenv("BRIAN_TEST_DSN"), "agent_status")
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Pool.Close()
	ids, err := restarted.StatusPending(context.Background())
	if err != nil || len(ids) != 1 || ids[0] != "create:"+good.ID {
		t.Fatal("cooldown lost or update overtook creation", ids, err)
	}
	if err = restarted.Complete(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(context.Background(), "UPDATE brian_agent_status.operations SET status_publish_after=clock_timestamp()-interval '1 second' WHERE id=$1", createID); err != nil {
		t.Fatal(err)
	}
	ids, err = restarted.StatusPending(context.Background())
	if err != nil || len(ids) != 1 || ids[0] != createID {
		t.Fatal(ids, err)
	}
	after, err := restarted.Operation(context.Background(), createID)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(before.Content)
	b, _ := json.Marshal(after.Content)
	if string(a) != string(b) || after.ID != before.ID || after.Done {
		t.Fatal("retry changed the operation")
	}
	if err = restarted.Complete(context.Background(), createID); err != nil {
		t.Fatal(err)
	}
	ids, err = restarted.StatusPending(context.Background())
	if err != nil || len(ids) != 1 || ids[0] != "status:"+bad.ID+":2" {
		t.Fatal("next ordered update missing", ids, err)
	}
}

func TestStatusRetryDoesNotStarveBehindMoreThanOneBatch(t *testing.T) {
	s := statusPublicationStore(t)
	for i := 0; i < 60; i++ {
		statusPublicationInstance(t, s, fmt.Sprintf("blocked-%d", i))
	}
	good := statusPublicationInstance(t, s, "healthy")
	ids, err := s.StatusPending(context.Background())
	if err != nil || len(ids) != 50 {
		t.Fatal(ids, err)
	}
	for _, id := range ids {
		if _, err = s.RetryStatus(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	next, err := s.StatusPending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range next {
		if id == "create:"+good.ID {
			return
		}
	}
	t.Fatal("eligible healthy instance remained behind the failed batch", next)
}

func TestStatusLostHTTPResponseRetriesTheSameJournalOperation(t *testing.T) {
	s := statusPublicationStore(t)
	i := statusPublicationInstance(t, s, "healthy")
	var mu sync.Mutex
	var deliveries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		op, err := s.Operation(r.Context(), operationID(r))
		if err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		content, _ := json.Marshal(op.Content)
		mu.Lock()
		deliveries = append(deliveries, op.ID+string(content))
		count := len(deliveries)
		mu.Unlock()
		// Model storage success followed by losing the publication HTTP response.
		if count == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	}))
	t.Cleanup(server.Close)
	startStatusPublisher(t, s, server)
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		op, err := s.Operation(context.Background(), "create:"+i.ID)
		if err != nil {
			t.Fatal(err)
		}
		if op.Done {
			mu.Lock()
			defer mu.Unlock()
			if len(deliveries) != 2 || deliveries[0] != deliveries[1] {
				t.Fatal("non-idempotent retry", deliveries)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("lost HTTP acknowledgement was not recovered")
}

func TestStatusSlowPublicationCannotHoldTheWholeCycle(t *testing.T) {
	s := statusPublicationStore(t)
	bad := statusPublicationInstance(t, s, "slow")
	good := statusPublicationInstance(t, s, "healthy")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		op, err := s.Operation(r.Context(), operationID(r))
		if err != nil {
			w.WriteHeader(500)
			return
		}
		if op.RoomID == bad.RoomID {
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
			case <-time.After(3 * time.Second):
			}
			return
		}
		w.WriteHeader(200)
	}))
	t.Cleanup(server.Close)
	startStatusPublisher(t, s, server)
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		op, err := s.Operation(context.Background(), "create:"+good.ID)
		if err != nil {
			t.Fatal(err)
		}
		if op.Done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("slow failed publication held the healthy room")
}
