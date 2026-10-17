package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
)

func TestPostgresRestartDedupRoomAndConcurrentApproval(t *testing.T) {
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("BRIAN_TEST_DSN required")
	}
	ctx := context.Background()
	s, e := Open(ctx, dsn, "approval")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Pool.Close()
	_, e = s.Pool.Exec(ctx, "TRUNCATE brian_approval.instances,brian_approval.operations,brian_approval.cursors")
	if e != nil {
		t.Fatal(e)
	}
	req := request(nil)
	i, e := s.Create(ctx, req)
	if e != nil {
		t.Fatal(e)
	}
	i2, e := s.Create(ctx, req)
	if e != nil || i2.ID != i.ID {
		t.Fatal("create not idempotent", e)
	}
	makeEvent := func(id, room, actor, cmd string) protocol.Event {
		b, _ := json.Marshal(protocol.Command{Plugin: "approval", InstanceID: i.ID, Command: cmd, Payload: map[string]any{}})
		return protocol.Event{EventID: id, RoomID: room, SenderID: actor, Type: "m.plugin.command", Content: b}
	}
	malformed := makeEvent("invalid", "room", "owner", "approve")
	malformed.Content = json.RawMessage(`{"plugin":"approval","instance_id":"","command":"approve","payload":null}`)
	invalid, e := s.Handle(ctx, malformed)
	if e != nil || invalid.EventType != "m.plugin.command.rejected" {
		t.Fatal("invalid envelope blocks journal", invalid, e)
	}
	wrong := makeEvent("wrong-room", "other", "owner", "approve")
	op, e := s.Handle(ctx, wrong)
	if e != nil || op.EventType != "m.plugin.command.rejected" {
		t.Fatal(op, e)
	}
	var wg sync.WaitGroup
	for _, id := range []string{"one", "two"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if _, e := s.Handle(ctx, makeEvent(id, "room", "owner", "approve")); e != nil {
				t.Error(e)
			}
		}(id)
	}
	wg.Wait()
	i, e = s.Get(ctx, i.ID)
	if e != nil || i.State["status"] != "approved" || revision(i.State) != 2 {
		t.Fatal(i, e)
	}
	// The webhook and PostgreSQL replay can encode the same JSON with different order/spacing.
	replay := makeEvent("one", "room", "owner", "approve")
	replay.Content = json.RawMessage(`{ "payload":{}, "command":"approve", "instance_id":"` + i.ID + `", "plugin":"approval" }`)
	if _, e = s.Handle(ctx, replay); e != nil {
		t.Fatal("equivalent JSON treated as different event", e)
	}
	restarted, e := Open(ctx, dsn, "approval")
	if e != nil {
		t.Fatal(e)
	}
	defer restarted.Pool.Close()
	for _, id := range []string{"one", "two"} {
		if _, e = restarted.Handle(ctx, makeEvent(id, "room", "owner", "approve")); e != nil {
			t.Fatal(e)
		}
	}
	i, e = restarted.Get(ctx, i.ID)
	if e != nil || revision(i.State) != 2 {
		t.Fatal("duplicate applied", i, e)
	}
	op, e = restarted.Operation(ctx, "create:"+i.ID)
	if e != nil || op.Done {
		t.Fatal("pending publication lost", op, e)
	}
}
func TestPostgresUnmodifiedChecklistRequest(t *testing.T) {
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("BRIAN_TEST_DSN required")
	}
	ctx := context.Background()
	s, e := Open(ctx, dsn, "checklist")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Pool.Close()
	_, e = s.Pool.Exec(ctx, "TRUNCATE brian_checklist.instances,brian_checklist.operations")
	if e != nil {
		t.Fatal(e)
	}
	data := map[string]any{"items": []any{map[string]any{"id": "a", "label": "A"}}}
	req := request(data)
	i, e := s.Create(ctx, req)
	if e != nil {
		t.Fatal(e)
	}
	j, e := s.Create(ctx, req)
	if e != nil || j.ID != i.ID {
		t.Fatal("repeat conflicts", e)
	}
}

func TestPostgresDelegationAtomicRevisionAuditAndReplay(t *testing.T) {
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("BRIAN_TEST_DSN required")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, "checklist")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	_, err = s.Pool.Exec(ctx, "TRUNCATE brian_checklist.instances,brian_checklist.operations")
	if err != nil {
		t.Fatal(err)
	}
	i, err := s.Create(ctx, request(map[string]any{"items": []any{map[string]any{"id": "a", "label": "A"}}}))
	if err != nil {
		t.Fatal(err)
	}
	source := protocol.Event{EventID: "request", RoomSeq: 10, RoomID: "room", SenderID: "owner", Type: "m.room.message", Content: json.RawMessage(`{"body":"Adicione B"}`)}
	offline := false
	nano := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if offline {
			w.WriteHeader(503)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"chunk": []protocol.Event{source}})
	}))
	defer nano.Close()
	r := Runtime{Store: s, Client: nano.Client(), NimletURL: nano.URL, NimletToken: "consumer", DelegatingAgents: map[string]bool{"agent": true}}
	rev := int64(1)
	command := protocol.Command{Plugin: "checklist", InstanceID: i.ID, Command: "add_item", Payload: map[string]any{"item_id": "b", "label": "B"}, ExpectedRevision: &rev, Delegation: &protocol.Delegation{RequestEventID: "request", RequestRoomSeq: 10}}
	body, _ := json.Marshal(command)
	e := protocol.Event{EventID: "command", RoomSeq: 11, RoomID: "room", SenderID: "agent", Type: "m.plugin.command", Content: body}
	op, err := r.Handle(ctx, e)
	if err != nil || op.EventType != "m.plugin.state.updated" {
		t.Fatal(op, err)
	}
	audit := op.Content["delegation"].(map[string]any)
	if audit["agent_id"] != "agent" || audit["requester_id"] != "owner" {
		t.Fatal(audit)
	}
	offline = true
	if _, err = r.Handle(ctx, e); err != nil {
		t.Fatal("committed replay needed Nimlet", err)
	}
	current, _ := s.Get(ctx, i.ID)
	if revision(current.State) != 2 || len(array(current.State, "items")) != 2 {
		t.Fatal("replay duplicated effect")
	}
	transient := e
	transient.EventID = "outage"
	if _, err = r.Handle(ctx, transient); err == nil {
		t.Fatal("outage permanently consumed command")
	}
	if _, err = s.Operation(ctx, "outage"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("outage entered journal", err)
	}
	offline = false
	stale := e
	stale.EventID = "stale"
	op, err = r.Handle(ctx, stale)
	if err != nil || op.EventType != "m.plugin.command.rejected" {
		t.Fatal("stale revision accepted", op, err)
	}
	foreign := e
	foreign.EventID = "foreign"
	source.SenderID = "other"
	op, err = r.Handle(ctx, foreign)
	if err != nil || op.EventType != "m.plugin.command.rejected" {
		t.Fatal("foreign requester accepted", op, err)
	}
	source.SenderID = "owner"
	cross := e
	cross.EventID = "cross"
	cross.RoomID = "other"
	op, err = r.Handle(ctx, cross)
	if err != nil || op.EventType != "m.plugin.command.rejected" {
		t.Fatal("cross room accepted", op, err)
	}
	untrusted := e
	untrusted.EventID = "untrusted"
	untrusted.SenderID = "attacker"
	op, err = r.Handle(ctx, untrusted)
	if err != nil || op.EventType != "m.plugin.command.rejected" {
		t.Fatal("untrusted agent accepted", op, err)
	}
}
