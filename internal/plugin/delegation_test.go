package plugin

import (
	"context"
	"encoding/json"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDelegationVerifiesOriginAndPreservesTransportActor(t *testing.T) {
	source := protocol.Event{EventID: "request", RoomSeq: 10, RoomID: "room", SenderID: "owner", Type: "m.room.message", Content: json.RawMessage(`{"body":"Adicione B","msgtype":"m.text"}`)}
	status := 200
	calls := 0
	nano := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		calls++
		if q.URL.Query().Get("from") != "9" || q.URL.Query().Get("limit") != "1" || q.Header.Get("Authorization") != "Bearer consumer" {
			t.Error("wrong origin boundary")
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"chunk": []protocol.Event{source}})
	}))
	defer nano.Close()
	r := Runtime{Store: &Store{Name: "checklist"}, Client: nano.Client(), NimletURL: nano.URL, NimletToken: "consumer", DelegatingAgents: map[string]bool{"agent": true}}
	rev := int64(1)
	cmd := protocol.Command{Plugin: "checklist", InstanceID: "instance", Command: "add_item", Payload: map[string]any{"item_id": "b", "label": "B"}, ExpectedRevision: &rev, Delegation: &protocol.Delegation{RequestEventID: "request", RequestRoomSeq: 10}}
	e := protocol.Event{EventID: "command", RoomSeq: 11, RoomID: "room", SenderID: "agent", Type: "m.plugin.command"}
	actor, denied, retry := r.delegationActor(context.Background(), e, cmd)
	if actor != "owner" || denied != nil || retry != nil || e.SenderID != "agent" {
		t.Fatal(actor, denied, retry)
	}
	source.EventID = "forged"
	_, denied, retry = r.delegationActor(context.Background(), e, cmd)
	if denied == nil || retry != nil {
		t.Fatal("forged origin accepted")
	}
	source.EventID = "request"
	source.RoomID = "other"
	_, denied, retry = r.delegationActor(context.Background(), e, cmd)
	if denied == nil || retry != nil {
		t.Fatal("foreign room accepted")
	}
	source.RoomID = "room"
	source.Type = "m.plugin.state.updated"
	_, denied, retry = r.delegationActor(context.Background(), e, cmd)
	if denied == nil || retry != nil {
		t.Fatal("non-message origin accepted")
	}
	source.Type = "m.room.message"
	status = 503
	_, denied, retry = r.delegationActor(context.Background(), e, cmd)
	if denied != nil || retry == nil {
		t.Fatal("temporary outage became rejection")
	}
	before := calls
	e.SenderID = "attacker"
	_, denied, retry = r.delegationActor(context.Background(), e, cmd)
	if denied == nil || retry != nil || calls != before {
		t.Fatal("untrusted agent fetched origin")
	}
	e.SenderID = "agent"
	r.Store.Name = "approval"
	cmd.Plugin = "approval"
	cmd.Command = "approve"
	_, denied, retry = r.delegationActor(context.Background(), e, cmd)
	if denied == nil || retry != nil || calls != before {
		t.Fatal("agent can approve")
	}
	r.Store.Name = "checklist"
	cmd.Plugin = "checklist"
	cmd.Command = "add_item"
	e.RoomSeq = 10
	_, denied, retry = r.delegationActor(context.Background(), e, cmd)
	if denied == nil || retry != nil {
		t.Fatal("future origin accepted")
	}
	e.RoomSeq = 11
	cmd.ExpectedRevision = nil
	_, denied, retry = r.delegationActor(context.Background(), e, cmd)
	if denied == nil || retry != nil {
		t.Fatal("missing revision accepted")
	}
}
