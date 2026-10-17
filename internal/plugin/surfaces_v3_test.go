package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"github.com/thiagoribeiro/agent-blocks/internal/roomadmin"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

func TestMonthlyBillsRolloverAndConfirmation(t *testing.T) {
	s, err := Initial("monthly_bills", request(map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	apply := func(cmd string, p map[string]any) {
		t.Helper()
		s, err = Apply("monthly_bills", s, "owner", "owner", cmd, p, now)
		if err != nil {
			t.Fatal(cmd, err)
		}
	}
	apply("add_bill", map[string]any{"item_id": "energy", "name": "Energia", "due_day": 31})
	apply("open_period", map[string]any{"period": "2026-09"})
	apply("record_payment", map[string]any{"period": "2026-09", "item_id": "energy", "amount_minor": 24250, "confirmed": true})
	apply("open_period", map[string]any{"period": "2026-10"})
	get := func(month string) map[string]any {
		return s["periods"].(map[string]any)[month].(map[string]any)["entries"].(map[string]any)["energy"].(map[string]any)
	}
	oct := get("2026-10")
	if revision(map[string]any{"revision": oct["suggested_amount_minor"]}) != 24250 || oct["status"] != "pending" || oct["confirmed_amount_minor"] != nil {
		t.Fatal(oct)
	}
	if _, err = Apply("monthly_bills", s, "owner", "owner", "record_payment", map[string]any{"period": "2026-10", "item_id": "energy", "amount_minor": 28740}, now); err == nil {
		t.Fatal("unconfirmed payment accepted")
	}
	if _, err = Apply("monthly_bills", s, "owner", "owner", "record_payment", map[string]any{"period": "2026-10", "item_id": "energy", "amount_minor": 28740, "confirmed": true, "paid_on": "2026-02-30"}, now); err == nil {
		t.Fatal("invalid payment date accepted")
	}
	apply("record_payment", map[string]any{"period": "2026-10", "item_id": "energy", "amount_minor": 28740, "confirmed": true, "paid_on": "2026-10-12"})
	if get("2026-10")["paid_on"] != "2026-10-12" || get("2026-10")["paid_by"] != "owner" {
		t.Fatal("payment attribution/date lost")
	}
	apply("open_period", map[string]any{"period": "2026-11"})
	if revision(map[string]any{"revision": get("2026-11")["suggested_amount_minor"]}) != 28740 {
		t.Fatal(get("2026-11"))
	}
	apply("open_period", map[string]any{"period": "2027-02"})
	if get("2027-02")["due_date"] != "2027-02-28" || get("2027-02")["suggestion_source_period"] != "2026-10" {
		t.Fatal(get("2027-02"))
	}
	before := hash(s["periods"])
	apply("open_period", map[string]any{"period": "2027-02"})
	if hash(s["periods"]) != before {
		t.Fatal("period changed on replay")
	}
	if _, ok := amount(28.74); ok {
		t.Fatal("float money accepted")
	}
}
func TestSurfaceACLAndRename(t *testing.T) {
	s, _ := Initial("checklist", request(map[string]any{"items": []any{map[string]any{"id": "a", "label": "A"}}}))
	if _, err := Apply("checklist", s, "owner", "outsider", "check_item", map[string]any{"item_id": "a"}, time.Now()); err != ErrForbidden {
		t.Fatal("membership alone authorized editing", err)
	}
	s["acl"].(map[string]any)["editor"] = "editor"
	n, err := Apply("checklist", s, "owner", "editor", "rename", map[string]any{"title": "Compras"}, time.Now())
	if err != nil || n["title"] != "Compras" {
		t.Fatal(n, err)
	}
	if _, err = Apply("checklist", s, "owner", "editor", "share", map[string]any{}, time.Now()); err != ErrForbidden {
		t.Fatal("editor can share")
	}
	if _, err = Apply("checklist", s, "owner", "editor", "archive", map[string]any{}, time.Now()); err != ErrForbidden {
		t.Fatal("editor can archive")
	}
}
func TestPostgresTransferRecoversAndPermanentlyFencesSource(t *testing.T) {
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("isolated BRIAN_TEST_DSN required")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, "checklist")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	req := request(map[string]any{"items": []any{map[string]any{"id": "a", "label": "A"}, map[string]any{"id": "b", "label": "B"}}})
	req.Key = fmt.Sprint(time.Now().UnixNano())
	req.RoomID = "11111111-1111-4111-8111-111111111111"
	i, err := s.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	event := func(key, room, actor, cmd string, p map[string]any) protocol.Event {
		b, _ := json.Marshal(protocol.Command{Plugin: "checklist", InstanceID: i.ID, Command: cmd, Payload: p})
		return protocol.Event{EventID: key + req.Key, RoomID: room, SenderID: actor, Type: "m.plugin.command", Content: b}
	}
	if _, err = s.Handle(ctx, event("check", req.RoomID, "owner", "check_item", map[string]any{"item_id": "a"})); err != nil {
		t.Fatal(err)
	}
	preview := map[string]any{"token": "reviewed", "invitees": []any{"guest"}, "participants": []any{map[string]any{"user_id": "owner", "username": "owner", "type": "human", "state": "join"}, map[string]any{"user_id": "editor", "username": "guest", "type": "human", "state": "invite"}}, "expires_at": time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)}
	if _, err = s.handlePrepared(ctx, event("preview", req.RoomID, "owner", "preview_share", map[string]any{}), "", nil, preview); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Handle(ctx, event("share", req.RoomID, "owner", "share", map[string]any{"preview_token": "reviewed"})); err != nil {
		t.Fatal(err)
	}
	fail := true
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			http.Error(w, "offline", 503)
			return
		}
		json.NewEncoder(w).Encode(roomadmin.Response{Prepared: &roomadmin.Prepared{RoomID: "22222222-2222-4222-8222-222222222222", Members: []roomadmin.Member{{UserID: "owner", State: "join"}, {UserID: "editor", State: "invite"}}}})
	}))
	defer admin.Close()
	runtime := &Runtime{Store: s, SharingEnabled: true, Admin: &roomadmin.Client{URL: admin.URL, Token: "test", HTTP: admin.Client()}}
	if err = runtime.Transfer(ctx, i.ID); err == nil {
		t.Fatal("expected failed preparation")
	}
	source, _ := s.Get(ctx, i.ID)
	if str(source.State["transfer"].(map[string]any), "phase") != "frozen" {
		t.Fatal(source)
	}
	op, err := s.Handle(ctx, event("late", req.RoomID, "owner", "uncheck_item", map[string]any{"item_id": "a"}))
	if err != nil || op.EventType != "m.plugin.command.rejected" {
		t.Fatal("source still writable", op, err)
	}
	fail = false
	var wg sync.WaitGroup
	for n := 0; n < 2; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runtime.Transfer(ctx, i.ID); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	source, _ = s.Get(ctx, i.ID)
	move := source.State["transfer"].(map[string]any)
	dest, err := s.Get(ctx, str(move, "instance_id"))
	if err != nil {
		t.Fatal(err)
	}
	if dest.RoomID != str(move, "room_id") || array(dest.State, "items")[0].(map[string]any)["checked"] != true || dest.State["created_at"] != source.State["created_at"] {
		t.Fatal(dest)
	}
	if dest.State["transfer"] != nil || !CanEdit(dest.State, "owner", "editor") {
		t.Fatal(dest.State)
	}
	var count int
	if err = s.Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE source_id=$1", s.table("transfers")), i.ID).Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	// Neither a second share nor an offline retry can reactivate the origin.
	op, err = s.Handle(ctx, event("late2", req.RoomID, "owner", "add_item", map[string]any{"item_id": "c", "label": "C"}))
	if err != nil || op.EventType != "m.plugin.command.rejected" {
		t.Fatal(op, err)
	}
	if err = runtime.Transfer(ctx, i.ID); err != nil {
		t.Fatal(err)
	}
	// Crash after the authority cut and a partial publication: a new process must
	// finish the same outbox, never create another destination or unlock the source.
	resumed, err := Open(ctx, dsn, "checklist")
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Pool.Close()
	ids := []string{"create:" + dest.ID, "moved:" + str(move, "operation_id"), "redirect:" + str(move, "operation_id")}
	if err = resumed.Complete(ctx, ids[0]); err != nil {
		t.Fatal(err)
	}
	restarted := &Runtime{Store: resumed, Admin: runtime.Admin}
	if err = restarted.Transfer(ctx, i.ID); err != nil {
		t.Fatal(err)
	}
	var phase string
	if err = resumed.Pool.QueryRow(ctx, fmt.Sprintf("SELECT phase FROM %s WHERE source_id=$1", resumed.table("transfers")), i.ID).Scan(&phase); err != nil || phase != "committed" {
		t.Fatal(phase, err)
	}
	for _, opID := range ids[1:] {
		if err = resumed.Complete(ctx, opID); err != nil {
			t.Fatal(err)
		}
	}
	if err = resumed.Pool.QueryRow(ctx, fmt.Sprintf("SELECT phase FROM %s WHERE source_id=$1", resumed.table("transfers")), i.ID).Scan(&phase); err != nil || phase != "completed" {
		t.Fatal(phase, err)
	}
	source, _ = resumed.Get(ctx, i.ID)
	if str(source.State["transfer"].(map[string]any), "phase") != "moved" {
		t.Fatal("publication recovery unlocked origin")
	}
}

func TestExportNeverIncludesPrivateTransportContext(t *testing.T) {
	state := map[string]any{"title": "List", "owner_id": "owner", "status": "open", "revision": 1, "source_room_id": "secret", "request_event_id": "private", "transfer": map[string]any{"phase": "frozen"}, "items": []any{map[string]any{"id": "a", "label": "A", "checked": true, "source_event_id": "private-event"}}}
	out := exportState("checklist", state)
	if out["source_room_id"] != nil || out["request_event_id"] != nil || out["transfer"] != nil || array(out, "items")[0].(map[string]any)["source_event_id"] != nil {
		t.Fatal(out)
	}
	if array(out, "items")[0].(map[string]any)["checked"] != true {
		t.Fatal("lost business state")
	}
}
func TestPostgresInvalidCreationIsJournaledWithoutPoisoningCursor(t *testing.T) {
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("isolated BRIAN_TEST_DSN required")
	}
	s, err := Open(context.Background(), dsn, "checklist")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	r := &Runtime{Store: s}
	key := fmt.Sprint(time.Now().UnixNano())
	body, _ := json.Marshal(protocol.Command{Plugin: "checklist", InstanceID: "create:" + key, Command: "create_shared", Payload: map[string]any{"title": "List", "data": map[string]any{}}})
	event := protocol.Event{EventID: key, RoomID: "room", SenderID: "owner", Type: "m.plugin.command", Content: body}
	op, err := r.Handle(context.Background(), event)
	if err != nil || op.EventType != "m.plugin.command.rejected" {
		t.Fatal(op, err)
	}
	replay, err := r.Handle(context.Background(), event)
	if err != nil || hash(op.Content) != hash(replay.Content) {
		t.Fatal("replay changed", err)
	}
}

func TestPostgresSharingSnapshotSurvivesFailedProvisioning(t *testing.T) {
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("isolated BRIAN_TEST_DSN required")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, "checklist")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	checks, attempts := 0, 0
	original := []roomadmin.Member{{UserID: "owner", Username: "Alice", Type: "human", State: "join"}, {UserID: "guest", Username: "Bob", Type: "human", State: "invite"}}
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var q roomadmin.Request
		json.NewDecoder(req.Body).Decode(&q)
		if q.Action == "validate" {
			checks++
			json.NewEncoder(w).Encode(roomadmin.Response{Members: original})
			return
		}
		attempts++
		if len(q.Participants) != 2 || q.Participants[1].UserID != "guest" {
			t.Error("lost captured selection", q.Participants)
		}
		if attempts == 1 {
			http.Error(w, "offline", 503)
			return
		}
		json.NewEncoder(w).Encode(roomadmin.Response{Prepared: &roomadmin.Prepared{RoomID: "destination", Members: q.Participants}})
	}))
	defer admin.Close()
	runtime := &Runtime{Store: s, Admin: &roomadmin.Client{URL: admin.URL, Token: "test", HTTP: admin.Client()}}
	key := fmt.Sprint(time.Now().UnixNano())
	if _, err = runtime.prepareShared(ctx, "source", "owner", key, []any{}); err == nil {
		t.Fatal("expected interruption")
	}
	// The origin has different participants now. A fresh worker must use the admitted selection.
	original = []roomadmin.Member{{UserID: "newcomer", Username: "Carol", Type: "human", State: "join"}}
	restarted := &Runtime{Store: s, Admin: runtime.Admin}
	if _, err = restarted.prepareShared(ctx, "source", "owner", key, []any{}); err != nil {
		t.Fatal(err)
	}
	if checks != 1 || attempts != 2 {
		t.Fatal("membership selection was recaptured", checks, attempts)
	}
}

func TestShareRequiresMatchingUnexpiredPreview(t *testing.T) {
	now := time.Now()
	s := map[string]any{"title": "List", "status": "open", "revision": 1, "owner_id": "owner", "items": []any{}}
	if _, err := Apply("checklist", s, "owner", "owner", "share", map[string]any{"invitees": []any{}}, now); err == nil {
		t.Fatal("share without review")
	}
	p := map[string]any{"token": "reviewed", "invitees": []any{"guest"}, "participants": []any{}, "expires_at": now.Add(time.Minute).UTC().Format(time.RFC3339Nano)}
	reviewed, err := Apply("checklist", s, "owner", "owner", "preview_share", map[string]any{"_validated_preview": p}, now)
	if err != nil {
		t.Fatal(err)
	}
	if reviewed["transfer"] != nil {
		t.Fatal("preview started transfer")
	}
	for _, payload := range []map[string]any{{}, {"preview_token": "forged"}} {
		if _, err = Apply("checklist", reviewed, "owner", "owner", "share", payload, now); err == nil {
			t.Fatal("unconfirmed share")
		}
	}
	if _, err = Apply("checklist", reviewed, "owner", "owner", "share", map[string]any{"preview_token": "reviewed"}, now.Add(2*time.Minute)); err == nil {
		t.Fatal("expired preview")
	}
	out, err := Apply("checklist", reviewed, "owner", "owner", "share", map[string]any{"preview_token": "reviewed", "invitees": []any{"attacker"}}, now)
	if err != nil || array(out["transfer"].(map[string]any), "invitees")[0] != "guest" {
		t.Fatal("did not use reviewed audience", err)
	}
}

func TestPostgresSharingPreviewDoesNotProvisionOrTrustClientParticipants(t *testing.T) {
	dsn := os.Getenv("BRIAN_TEST_DSN")
	if dsn == "" {
		t.Skip("isolated BRIAN_TEST_DSN required")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn, "checklist")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	req := request(map[string]any{"items": []any{map[string]any{"id": "a", "label": "A"}}})
	req.Key = fmt.Sprint(time.Now().UnixNano())
	req.RoomID = "11111111-1111-4111-8111-111111111111"
	i, err := s.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	event := func(key, cmd string, p map[string]any) protocol.Event {
		b, _ := json.Marshal(protocol.Command{Plugin: "checklist", InstanceID: i.ID, Command: cmd, Payload: p})
		return protocol.Event{EventID: key + req.Key, RoomID: req.RoomID, SenderID: "owner", Type: "m.plugin.command", Content: b}
	}
	var provisions int
	members := []roomadmin.Member{{UserID: "owner", Username: "Owner", Type: "human", State: "join"}}
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q roomadmin.Request
		json.NewDecoder(r.Body).Decode(&q)
		if q.Action != "validate" {
			provisions++
		}
		json.NewEncoder(w).Encode(roomadmin.Response{Members: members})
	}))
	defer admin.Close()
	runtime := &Runtime{Store: s, SharingEnabled: true, Admin: &roomadmin.Client{URL: admin.URL, Token: "test", HTTP: admin.Client()}}
	op, err := runtime.Handle(ctx, event("empty", "preview_share", map[string]any{"invitees": []any{}}))
	if err != nil || op.EventType != "m.plugin.command.rejected" {
		t.Fatal("allowed self-only sharing", err)
	}
	op, err = s.Handle(ctx, event("forged", "preview_share", map[string]any{"_validated_preview": map[string]any{"token": "forged"}}))
	if err != nil || op.EventType != "m.plugin.command.rejected" {
		t.Fatal("trusted forged preview", err)
	}
	members = append(members, roomadmin.Member{UserID: "editor", Username: "Editor", Type: "human", State: "join"})
	op, err = runtime.Handle(ctx, event("review", "preview_share", map[string]any{"invitees": []any{}}))
	if err != nil || op.EventType != "m.plugin.state.updated" {
		t.Fatal("preview failed", err)
	}
	reviewed, _ := s.Get(ctx, i.ID)
	if reviewed.State["transfer"] != nil || provisions != 0 {
		t.Fatal("preview provisioned or transferred")
	}
	p := reviewed.State["sharing_preview"].(map[string]any)
	members = append(members, roomadmin.Member{UserID: "later", Username: "Later", Type: "human", State: "join"})
	share := event("share", "share", map[string]any{"preview_token": str(p, "token")})
	op, err = runtime.Handle(ctx, share)
	if err != nil || op.EventType != "m.plugin.state.updated" {
		t.Fatal("confirmed share failed", err)
	}
	var b []byte
	if err = s.Pool.QueryRow(ctx, fmt.Sprintf("SELECT participants FROM %s WHERE id=$1", s.table("sharing_participants")), sharingUUID(s.Name+":"+share.EventID)).Scan(&b); err != nil {
		t.Fatal(err)
	}
	var selected []roomadmin.Member
	json.Unmarshal(b, &selected)
	if len(selected) != 2 {
		t.Fatal("audience changed after review", string(b))
	}
}
