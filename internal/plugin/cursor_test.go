package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
)

func TestPostgresUpgradesLegacyNumericCursors(t *testing.T) {
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("BRIAN_TEST_DSN required")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, "poll")
	if err != nil {
		t.Fatal(err)
	}
	// Recreate the old schema: cursors were numbers.
	_, err = s.Pool.Exec(ctx, `DROP SCHEMA brian_poll CASCADE;
 CREATE SCHEMA brian_poll;
 CREATE TABLE brian_poll.instances (id text PRIMARY KEY, create_key text UNIQUE NOT NULL, room_id text NOT NULL, owner_id text NOT NULL, source_pos bigint NOT NULL, request jsonb NOT NULL, state jsonb NOT NULL);
 CREATE TABLE brian_poll.cursors (id text PRIMARY KEY, pos bigint NOT NULL);
 INSERT INTO brian_poll.cursors VALUES ('sync', 243), ('replay:room', 300);`)
	s.Pool.Close()
	if err != nil {
		t.Fatal(err)
	}
	for range 2 { // the upgrade runs once; reopening is a no-op
		s, err = Open(ctx, dsn, "poll")
		if err != nil {
			t.Fatal(err)
		}
		sync, _ := s.Cursor(ctx, "sync")
		replay, _ := s.Cursor(ctx, "replay:room")
		s.Pool.Close()
		if sync != "243" || replay != "" {
			t.Fatalf("sync=%q (kept for conversion) replay=%q (dropped)", sync, replay)
		}
	}
}

func TestPostgresSyncNeverRestartsSilently(t *testing.T) {
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("BRIAN_TEST_DSN required")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, "poll")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	if _, err = s.Pool.Exec(ctx, "TRUNCATE brian_poll.cursors"); err != nil {
		t.Fatal(err)
	}
	var calls []string
	nano := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		since, present := q.URL.Query()["since"]
		if !present {
			calls = append(calls, "<none>")
			_ = json.NewEncoder(w).Encode(map[string]any{"next_batch": "c1.first", "events": []any{}})
			return
		}
		calls = append(calls, since[0])
		w.WriteHeader(http.StatusBadRequest) // e.g. a cursor issued to another account
	}))
	defer nano.Close()
	r := Runtime{Store: s, Client: nano.Client(), NimletURL: nano.URL, NimletToken: "consumer"}

	if err = r.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Cursor(ctx, "sync"); got != "c1.first" {
		t.Fatalf("next_batch not saved verbatim: %q", got)
	}
	if err = r.Sync(ctx); err == nil {
		t.Fatal("a rejected cursor must stop the sync")
	}
	if got, _ := s.Cursor(ctx, "sync"); got != "c1.first" {
		t.Fatalf("rejected cursor was replaced: %q", got)
	}
	if err = s.SaveCursor(ctx, "sync", "243"); err != nil {
		t.Fatal(err)
	}
	if err = r.Sync(ctx); err == nil {
		t.Fatal("a legacy numeric cursor must not be sent")
	}
	if len(calls) != 2 || calls[0] != "<none>" || calls[1] != "c1.first" {
		t.Fatalf("calls = %v", calls)
	}
}

func TestPostgresRehashMakesLegacyJournalReplayable(t *testing.T) {
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("BRIAN_TEST_DSN required")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, "approval")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	if _, err = s.Pool.Exec(ctx, "TRUNCATE brian_approval.instances,brian_approval.operations"); err != nil {
		t.Fatal(err)
	}
	req := request(nil)
	req.SourcePos = 500 // legacy global stream position
	i, err := s.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(protocol.Command{Plugin: "approval", InstanceID: i.ID, Command: "approve", Payload: map[string]any{}})
	cmd := protocol.Event{EventID: "cmd", RoomSeq: 7, RoomID: i.RoomID, SenderID: i.OwnerID, Type: "m.plugin.command", Content: b}
	if _, err = s.Handle(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	// What the cutover leaves behind: hashes in the old formula, source_pos remapped in the
	// column but not in the stored request.
	if _, err = s.Pool.Exec(ctx, "UPDATE brian_approval.operations SET input_hash='legacy' WHERE id NOT LIKE 'create:%'; UPDATE brian_approval.instances SET source_pos=5"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Handle(ctx, cmd); !errors.Is(err, ErrConflict) {
		t.Fatalf("legacy hash should conflict before the rehash, got %v", err)
	}

	rehashed, skipped, err := s.Rehash(ctx, map[string]protocol.Event{"cmd": cmd})
	if err != nil || rehashed != 1 || skipped != 0 {
		t.Fatalf("rehash: %d %d %v", rehashed, skipped, err)
	}
	replay := cmd
	replay.RoomSeq = 70 // the position does not identify the command
	if op, err := s.Handle(ctx, replay); err != nil || op.EventType != "m.plugin.state.updated" {
		t.Fatalf("replay after rehash: %v %v", op, err)
	}
	req.SourcePos = 5
	if again, err := s.Create(ctx, req); err != nil || again.ID != i.ID {
		t.Fatalf("repeated create with the remapped source_pos: %v", err)
	}
}
