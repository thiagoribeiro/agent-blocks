package plugin

import (
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"testing"
	"time"
)

func request(data map[string]any) protocol.Create {
	return protocol.Create{Key: "key", RoomID: "room", OwnerID: "owner", Title: "Test", ExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339), Data: data}
}
func TestApprovalOwnershipAndDeadline(t *testing.T) {
	s, err := Initial("approval", request(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, e := Apply("approval", s, "owner", "other", "approve", map[string]any{}, time.Now()); e != ErrForbidden {
		t.Fatal("foreign approval accepted", e)
	}
	n, err := Apply("approval", s, "owner", "owner", "approve", map[string]any{}, time.Now())
	if err != nil || n["status"] != "approved" || n["resolved_by"] != "owner" {
		t.Fatal(n, err)
	}
	if _, e := Apply("approval", n, "owner", "owner", "reject", nil, time.Now()); e != ErrConflict {
		t.Fatal("resolved twice", e)
	}
	n, err = Apply("approval", s, "owner", "owner", "approve", nil, time.Now().Add(2*time.Hour))
	if err != nil || n["status"] != "expired" {
		t.Fatal(n, err)
	}
}
func TestChecklistCreateDoesNotMutateAndReopens(t *testing.T) {
	data := map[string]any{"items": []any{map[string]any{"id": "a", "label": "A"}}}
	s, err := Initial("checklist", request(data))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := data["items"].([]any)[0].(map[string]any)["checked"]; ok {
		t.Fatal("create mutated request")
	}
	s["acl"].(map[string]any)["member"] = "editor"
	n, e := Apply("checklist", s, "owner", "member", "check_item", map[string]any{"item_id": "a"}, time.Now())
	if e != nil || n["status"] != "completed" {
		t.Fatal(n, e)
	}
	n, e = Apply("checklist", n, "owner", "member", "uncheck_item", map[string]any{"item_id": "a"}, time.Now())
	if e != nil || n["status"] != "open" {
		t.Fatal(n, e)
	}
	if _, e = Apply("checklist", n, "owner", "member", "archive", nil, time.Now()); e != ErrForbidden {
		t.Fatal(e)
	}
}
func TestPollReplacesOwnVote(t *testing.T) {
	s, e := Initial("poll", request(map[string]any{"options": []any{map[string]any{"id": "a", "label": "A"}, map[string]any{"id": "b", "label": "B"}}}))
	if e != nil {
		t.Fatal(e)
	}
	for _, id := range []string{"a", "b"} {
		s, e = Apply("poll", s, "owner", "member", "vote", map[string]any{"option_id": id}, time.Now())
		if e != nil {
			t.Fatal(e)
		}
	}
	opts := array(s, "options")
	if opts[0].(map[string]any)["votes"] != 0 || opts[1].(map[string]any)["votes"] != 1 {
		t.Fatal(opts)
	}
}
func TestFormInvalidSubmissionAndScope(t *testing.T) {
	s, e := Initial("form_stateful", request(map[string]any{"fields": []any{map[string]any{"id": "x", "label": "X", "type": "select", "required": true, "options": []any{"a", "b"}}}}))
	if e != nil {
		t.Fatal(e)
	}
	n, e := Apply("form_stateful", s, "owner", "owner", "submit", map[string]any{"values": map[string]any{"x": map[string]any{"bad": true}}}, time.Now())
	if e != nil || n["status"] != "invalid" {
		t.Fatal(n, e)
	}
	n, e = Apply("form_stateful", n, "owner", "owner", "submit", map[string]any{"values": map[string]any{"x": "a"}}, time.Now())
	if e != nil || n["status"] != "submitted" {
		t.Fatal(n, e)
	}
	if _, e = Apply("form_stateful", s, "owner", "other", "submit", map[string]any{"values": map[string]any{}}, time.Now()); e != ErrForbidden {
		t.Fatal(e)
	}
}
