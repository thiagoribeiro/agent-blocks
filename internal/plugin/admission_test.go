package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTrama records workflow runs and answers status lookups.
type fakeTrama struct {
	mu       sync.Mutex
	runs     map[string]int // operation id → POST /workflows/run count
	gets     atomic.Int32
	failNext atomic.Int32 // fail this many runs with 503
	status   map[string]string
}

func (f *fakeTrama) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == "POST" && r.URL.Path == "/workflows/run":
		if f.failNext.Add(-1) >= 0 {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		var body struct {
			Definition map[string]any `json:"definition"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		nodes, _ := body.Definition["nodes"].([]any)
		action := nodes[0].(map[string]any)["action"].(map[string]any)["request"].(map[string]any)
		// .../operations/{id}/publish
		parts := strings.Split(action["url"].(string), "/")
		id := parts[len(parts)-2]
		f.mu.Lock()
		f.runs[id]++
		n := f.runs[id]
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"id": fmt.Sprintf("exec-%s-%d", id, n)})
	case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/workflows/"):
		f.gets.Add(1)
		f.mu.Lock()
		status := f.status[strings.TrimPrefix(r.URL.Path, "/workflows/")]
		f.mu.Unlock()
		if status == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeTrama) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.runs {
		n += c
	}
	return n
}

func admissionRuntime(t *testing.T) (*Runtime, *fakeTrama, func(...string)) {
	t.Helper()
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("BRIAN_TEST_DSN required")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, "poll")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Pool.Close)
	if _, err = s.Pool.Exec(ctx, "TRUNCATE brian_poll.operations"); err != nil {
		t.Fatal(err)
	}
	trama := &fakeTrama{runs: map[string]int{}, status: map[string]string{}}
	srv := httptest.NewServer(trama)
	t.Cleanup(srv.Close)
	r := &Runtime{Store: s, Client: srv.Client(), TramaURL: srv.URL, RouterURL: "http://router", PublicURL: "http://plugin", Token: "secret", Wake: make(chan struct{}, 1), Admissions: make(chan struct{}, 1)}
	insert := func(ids ...string) {
		for _, id := range ids {
			if _, err := s.Pool.Exec(ctx, "INSERT INTO brian_poll.operations(id,input_hash,room_id,instance_id,event_type,content) VALUES($1,'h','room','poll','m.plugin.state.updated','{}')", id); err != nil {
				t.Fatal(err)
			}
		}
		r.Notify()
	}
	return r, trama, insert
}

func waitFor(t *testing.T, what string, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for " + what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func ids(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%02d", prefix, i)
	}
	return out
}

func TestPostgresBurstIsAdmittedOncePerOperationWithoutPolling(t *testing.T) {
	r, trama, insert := admissionRuntime(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.RunAdmissions(ctx, 1)
	started := time.Now()
	insert(ids("burst", 10)...)
	waitFor(t, "10 admissions", 3*time.Second, func() bool { return trama.total() == 10 })
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("burst took %s", elapsed)
	}
	if trama.gets.Load() != 0 {
		t.Fatalf("admission polled Trama %d times", trama.gets.Load())
	}
	var tracked int
	if err := r.Store.Pool.QueryRow(ctx, "SELECT count(*) FROM brian_poll.operations WHERE admitted_at IS NOT NULL AND jsonb_array_length(executions)=1").Scan(&tracked); err != nil || tracked != 10 {
		t.Fatal("tracked", tracked, err)
	}
}

func TestPostgresFailedAdmissionDoesNotBlockOthersAndRetries(t *testing.T) {
	r, trama, insert := admissionRuntime(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trama.failNext.Store(1)
	r.RunAdmissions(ctx, 1)
	insert("first", "second", "third")
	waitFor(t, "second and third", 2*time.Second, func() bool {
		trama.mu.Lock()
		defer trama.mu.Unlock()
		return trama.runs["second"] == 1 && trama.runs["third"] == 1
	})
	// "first" failed once and comes back after its 1 s backoff.
	waitFor(t, "first retried", 4*time.Second, func() bool {
		trama.mu.Lock()
		defer trama.mu.Unlock()
		return trama.runs["first"] == 1
	})
	var failures int
	if err := r.Store.Pool.QueryRow(ctx, "SELECT admission_failures FROM brian_poll.operations WHERE id='first'").Scan(&failures); err != nil || failures != 1 {
		t.Fatal("failures", failures, err)
	}
}

func TestPostgresTramaFailureCallbackRequeuesWithBackoff(t *testing.T) {
	r, trama, insert := admissionRuntime(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.RunAdmissions(ctx, 1)
	insert("op")
	waitFor(t, "admission", time.Second, func() bool { return trama.total() == 1 })
	if err := r.PublicationFailed(ctx, "op"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if trama.total() != 1 {
		t.Fatal("requeued operation ignored its backoff")
	}
	waitFor(t, "re-admission", 3*time.Second, func() bool { return trama.total() == 2 })
}

func TestPostgresSweepRequeuesFailedAndCompletesSucceeded(t *testing.T) {
	r, trama, insert := admissionRuntime(t)
	ctx := context.Background()
	insert("failed", "succeeded", "running", "fresh")
	for id, exec := range map[string]string{"failed": "x-failed", "succeeded": "x-ok", "running": "x-run"} {
		if _, err := r.Store.Pool.Exec(ctx, "UPDATE brian_poll.operations SET admitted_at=now()-interval '5 minutes', executions=jsonb_build_array($2::text) WHERE id=$1", id, exec); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Store.Pool.Exec(ctx, "UPDATE brian_poll.operations SET admitted_at=now(), executions='[\"x-fresh\"]' WHERE id='fresh'"); err != nil {
		t.Fatal(err)
	}
	trama.status = map[string]string{"x-failed": "FAILED", "x-ok": "SUCCEEDED", "x-run": "RUNNING", "x-fresh": "RUNNING"}
	r.Sweep(ctx)
	if trama.gets.Load() != 3 {
		t.Fatalf("sweep asked Trama %d times, want 3 (fresh operations are skipped)", trama.gets.Load())
	}
	state := map[string]string{}
	rows, err := r.Store.Pool.Query(ctx, "SELECT id, done, admitted_at IS NULL FROM brian_poll.operations")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		var done, waiting bool
		if err := rows.Scan(&id, &done, &waiting); err != nil {
			t.Fatal(err)
		}
		state[id] = fmt.Sprint(done, waiting)
	}
	want := map[string]string{"failed": "false true", "succeeded": "true false", "running": "false false", "fresh": "false false"}
	for id, w := range want {
		if state[id] != w {
			t.Errorf("%s: done/waiting = %s, want %s", id, state[id], w)
		}
	}
}

func TestPostgresConcurrentWorkersNeverAdmitTwice(t *testing.T) {
	r, trama, insert := admissionRuntime(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.RunAdmissions(ctx, 4)
	insert(ids("parallel", 20)...)
	waitFor(t, "20 admissions", 3*time.Second, func() bool { return trama.total() == 20 })
	time.Sleep(100 * time.Millisecond)
	trama.mu.Lock()
	defer trama.mu.Unlock()
	for id, n := range trama.runs {
		if n != 1 {
			t.Errorf("%s admitted %d times", id, n)
		}
	}
}

func TestResultDefinitionReportsExhaustedRetries(t *testing.T) {
	r := &Runtime{Store: &Store{Name: "poll"}, RouterURL: "http://router", PublicURL: "http://plugin/", Token: "secret"}
	def := r.ResultDefinition("op 1")
	hook, ok := def["onFailureCallback"].(map[string]any)
	if !ok || hook["url"] != "http://plugin/operations/op%201/failed" || hook["verb"] != "POST" {
		t.Fatalf("onFailureCallback = %#v", def["onFailureCallback"])
	}
	nodes := def["nodes"].([]any)
	if len(nodes) != 2 || nodes[1].(map[string]any)["id"] != "complete" {
		t.Fatal("publish and complete must stay retried workflow tasks")
	}
}
