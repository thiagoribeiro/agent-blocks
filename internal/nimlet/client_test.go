package nimlet

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
)

func TestSendEventUsesAppServicePathAndToken(t *testing.T) {
	var gotPath, gotAuth, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth, gotMethod = r.URL.EscapedPath(), r.Header.Get("Authorization"), r.Method
		_ = json.NewEncoder(w).Encode(SendEventResponse{EventID: "$e", RoomSeq: 7})
	}))
	defer srv.Close()

	res, err := New(srv.URL+"/", "as-token").SendEvent(context.Background(), "agent-blocks", "room/1", "m.ui.message", "plugin:poll:op", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if res.EventID != "$e" || res.RoomSeq != 7 {
		t.Fatalf("unexpected response %+v", res)
	}
	if gotMethod != http.MethodPut || gotAuth != "Bearer as-token" ||
		gotPath != "/appservice/agent-blocks/rooms/room%2F1/send/m.ui.message/plugin:poll:op" {
		t.Fatalf("unexpected request %s %s %q", gotMethod, gotPath, gotAuth)
	}
}

func TestSendEventReturnsUpstreamStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "private details", http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := New(srv.URL, "as-token").SendEvent(context.Background(), "agent-blocks", "room", "m.ui.message", "txn", json.RawMessage(`{}`))
	var upstream *protocol.HTTPError
	if !errors.As(err, &upstream) || upstream.Status != http.StatusForbidden {
		t.Fatalf("expected upstream 403, got %v", err)
	}
}
