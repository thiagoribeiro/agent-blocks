package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
)

func emailView() map[string]any {
	return map[string]any{
		"updated_at": "2026-11-13T12:00:00-03:00",
		"important": []any{map[string]any{"id": "msg:1", "from": "Banco", "subject": "Fatura", "received_at": "2026-11-13T10:00:00-03:00",
			"priority": "act", "due": "2026-11-15", "amount_cents": float64(12345)}},
		"recent": []any{map[string]any{"id": "msg:2", "from": "Loja", "subject": "Pedido", "received_at": "2026-11-13T11:00:00-03:00"}},
		"cleanup": map[string]any{
			"review":  []any{map[string]any{"id": "grp:a", "name": "joao silva pereira", "count": float64(3)}},
			"wrong":   []any{map[string]any{"id": "grp:b", "name": "joao souza lima", "count": float64(9)}},
			"batches": []any{map[string]any{"id": "20261113-090839-archive-wrong", "kind": "archive", "count": float64(10)}},
		},
	}
}

func emailState(t *testing.T) map[string]any {
	t.Helper()
	s, err := Initial("email_triage", protocol.Create{Key: "k", RoomID: "room", OwnerID: "owner", Title: "Email", Data: map[string]any{"view": emailView()}})
	if err != nil {
		t.Fatal(err)
	}
	return clone(s)
}

func TestEmailViewIsBounded(t *testing.T) {
	if validateEmailView(emailView()) != nil {
		t.Fatal("valid view rejected")
	}
	bad := []func(v map[string]any){
		func(v map[string]any) { v["body"] = "texto do email" },                                             // unknown key
		func(v map[string]any) { v["notice"] = strings.Repeat("x", emailStringMax+1) },                      // long string
		func(v map[string]any) { v["important"] = append(array(v, "important"), array(v, "important")[0]) }, // duplicate id
		func(v map[string]any) { v["recent"] = []any{map[string]any{"from": "x"}} },                         // no id
		func(v map[string]any) { v["cleanup"] = "x" },
		func(v map[string]any) {
			deep := map[string]any{}
			cur := deep
			for i := 0; i < 10; i++ {
				n := map[string]any{}
				cur["x"] = n
				cur = n
			}
			v["cleanup"] = deep
		},
	}
	for i, mutate := range bad {
		v := clone(emailView())
		mutate(v)
		if validateEmailView(v) == nil {
			t.Fatal("accepted bad view", i)
		}
	}
}

func TestEmailDecisionsOwnerOnlyTargetInViewNoDuplicates(t *testing.T) {
	s := emailState(t)
	now := time.Now()
	if _, err := Apply("email_triage", s, "owner", "someone", "mark_done", map[string]any{"item_id": "msg:1"}, now); !errors.Is(err, ErrForbidden) {
		t.Fatal("non-owner decided", err)
	}
	if _, err := Apply("email_triage", s, "owner", "owner", "mark_done", map[string]any{"item_id": "msg:404"}, now); err == nil {
		t.Fatal("decided about something not in view")
	}
	if _, err := Apply("email_triage", s, "owner", "owner", "group_not_mine", map[string]any{"group_id": "grp:b"}, now); err == nil {
		t.Fatal("group_not_mine reached an already-wrong group")
	}
	if _, err := Apply("email_triage", s, "owner", "owner", "undo_batch", map[string]any{"batch_id": "20261113-090839-archive-wrong"}, now); err == nil {
		t.Fatal("undo without confirmation")
	}
	if _, err := Apply("email_triage", s, "owner", "owner", "mark_done", map[string]any{"item_id": "msg:1", "x": 1.0}, now); !errors.Is(err, ErrInvalid) {
		t.Fatal("extra payload accepted", err)
	}
	if _, err := Apply("email_triage", s, "owner", "owner", "delete_mail", map[string]any{"item_id": "msg:1"}, now); err == nil {
		t.Fatal("unknown command accepted")
	}
	n, err := Apply("email_triage", s, "owner", "owner", "group_mine", map[string]any{"group_id": "grp:b"}, now)
	if err != nil {
		t.Fatal(err)
	}
	d := array(n, "decisions")
	if len(d) != 1 || str(d[0].(map[string]any), "id") != "d2" || str(d[0].(map[string]any), "state") != "pending" || revision(n) != 2 {
		t.Fatal("decision not recorded", n)
	}
	if _, err = Apply("email_triage", n, "owner", "owner", "group_unsure", map[string]any{"group_id": "grp:b"}, now); err == nil {
		t.Fatal("second pending decision about the same target")
	}
	n, err = Apply("email_triage", n, "owner", "owner", "undo_batch", map[string]any{"batch_id": "20261113-090839-archive-wrong", "confirmed": true}, now)
	if err != nil || len(array(n, "decisions")) != 2 {
		t.Fatal(err)
	}
}

func TestEmailIsNeitherDelegableNorRenamableNorShareable(t *testing.T) {
	if len(delegatedCommands["email_triage"]) != 0 {
		t.Fatal("email decisions must not be delegable to an agent")
	}
	s := emailState(t)
	for _, c := range []string{"rename", "preview_share", "share", "invite_participant"} {
		if _, err := Apply("email_triage", s, "owner", "owner", c, map[string]any{"title": "x"}, time.Now()); !errors.Is(err, ErrForbidden) {
			t.Fatal(c, err)
		}
	}
	if transferable("email_triage") {
		t.Fatal("email surface must stay personal")
	}
}

func TestEmailProducerResolvesDecisionsAndReplaysAreIdempotent(t *testing.T) {
	s := emailState(t)
	now := time.Now()
	s, _ = Apply("email_triage", s, "owner", "owner", "mark_done", map[string]any{"item_id": "msg:1"}, now)
	s, _ = Apply("email_triage", s, "owner", "owner", "group_mine", map[string]any{"group_id": "grp:a"}, now)
	u := EmailUpdate{Sequence: 1, View: emailView(), Decisions: []map[string]any{{"id": "d2", "state": "applied"}, {"id": "d3", "state": "failed", "error": "imap"}}}
	n, err := resolveEmail(s, u, now)
	if err != nil {
		t.Fatal(err)
	}
	d := array(n, "decisions")
	if str(d[0].(map[string]any), "state") != "applied" || str(d[1].(map[string]any), "error") != "imap" || number(n, "producer_sequence") != 1 {
		t.Fatal(d)
	}
	if _, err = resolveEmail(n, EmailUpdate{Sequence: 2, Decisions: []map[string]any{{"id": "d2", "state": "applied"}}}, now); err != nil {
		t.Fatal("replayed outcome rejected", err)
	}
	if _, err = resolveEmail(n, EmailUpdate{Sequence: 2, Decisions: []map[string]any{{"id": "d2", "state": "failed"}}}, now); !errors.Is(err, ErrConflict) {
		t.Fatal("outcome flipped", err)
	}
	for _, bad := range []map[string]any{{"id": "d404", "state": "applied"}, {"id": "d2", "state": "pending"}, {"id": "d2", "state": "applied", "note": "x"}} {
		if _, err = resolveEmail(s, EmailUpdate{Sequence: 2, Decisions: []map[string]any{bad}}, now); !errors.Is(err, ErrInvalid) {
			t.Fatal("bad outcome accepted", bad, err)
		}
	}
	// a decision nobody picks up expires
	late, err := resolveEmail(s, EmailUpdate{Sequence: 2, View: emailView()}, now.Add(8*24*time.Hour))
	if err != nil || str(array(late, "decisions")[0].(map[string]any), "error") != "expired" {
		t.Fatal("stale decision kept pending", err)
	}
}

func TestEmailResolvedDecisionsAreTrimmedPendingKept(t *testing.T) {
	all := []any{map[string]any{"id": "p", "state": "pending"}}
	for i := 0; i < emailResolvedKeep+20; i++ {
		all = append(all, map[string]any{"id": "r", "state": "applied"})
	}
	out := trimEmailDecisions(all)
	if len(out) != emailResolvedKeep+1 || str(out[0].(map[string]any), "state") != "pending" {
		t.Fatal(len(out))
	}
}

func TestEmailFallbackHasNoMailContent(t *testing.T) {
	f := Fallback(emailState(t))
	if strings.Contains(f, "Fatura") || strings.Contains(f, "Banco") || !strings.Contains(f, "1 importante") {
		t.Fatal(f)
	}
}

func TestPostgresEmailTriageProducerAndOwnerDecisions(t *testing.T) {
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("BRIAN_TEST_DSN required")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, "email_triage")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	if _, err = s.Pool.Exec(ctx, "TRUNCATE brian_email_triage.instances,brian_email_triage.operations,brian_email_triage.cursors"); err != nil {
		t.Fatal(err)
	}
	r := &Runtime{Store: s, Wake: make(chan struct{}, 1), Admissions: make(chan struct{}, 1), Producers: map[string]bool{"@r2d2": true}}
	i, err := s.Create(ctx, protocol.Create{Key: "email", RoomID: "room", OwnerID: "owner", Title: "Email", Data: map[string]any{"view": emailView()}})
	if err != nil {
		t.Fatal(err)
	}
	cmd := func(id, sender, command string, payload map[string]any) *protocol.Operation {
		b, _ := json.Marshal(protocol.Command{Plugin: "email_triage", InstanceID: i.ID, Command: command, Payload: payload})
		op, err := s.Handle(ctx, protocol.Event{EventID: id, RoomID: "room", SenderID: sender, Type: "m.plugin.command", Content: b})
		if err != nil {
			t.Fatal(err)
		}
		return op
	}
	if op := cmd("e1", "intruder", "mark_done", map[string]any{"item_id": "msg:1"}); op.EventType != "m.plugin.command.rejected" {
		t.Fatal("non-owner decision accepted")
	}
	if op := cmd("e2", "owner", "mark_done", map[string]any{"item_id": "msg:1"}); op.EventType != "m.plugin.state.updated" {
		t.Fatal("owner decision rejected", op)
	}
	u := EmailUpdate{RoomID: "room", OwnerID: "owner", ProducerID: "@r2d2", Sequence: 1, View: emailView(), Decisions: []map[string]any{{"id": "d2", "state": "applied"}}}
	if _, err = r.UpdateEmailTriage(ctx, i.ID, EmailUpdate{RoomID: "room", OwnerID: "owner", ProducerID: "@llm", Sequence: 1, View: emailView()}); !errors.Is(err, ErrForbidden) {
		t.Fatal("unknown producer accepted", err)
	}
	if _, err = r.UpdateEmailTriage(ctx, i.ID, EmailUpdate{RoomID: "other", OwnerID: "owner", ProducerID: "@r2d2", Sequence: 1, View: emailView()}); !errors.Is(err, ErrForbidden) {
		t.Fatal("other room accepted", err)
	}
	got, err := r.UpdateEmailTriage(ctx, i.ID, u)
	if err != nil || str(array(got.State, "decisions")[0].(map[string]any), "state") != "applied" {
		t.Fatal(err, got)
	}
	again, err := r.UpdateEmailTriage(ctx, i.ID, u)
	if err != nil || revision(again.State) != revision(got.State) {
		t.Fatal("lost-response replay not idempotent", err)
	}
	changed := u
	changed.View = map[string]any{}
	if _, err = r.UpdateEmailTriage(ctx, i.ID, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("same sequence, different content accepted", err)
	}
	op, err := s.Operation(ctx, "producer:"+i.ID+":1")
	if err != nil || op.EventType != "m.plugin.state.updated" {
		t.Fatal("producer update not journaled for publication", err)
	}
}
