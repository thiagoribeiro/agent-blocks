package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"github.com/thiagoribeiro/agent-blocks/internal/roomadmin"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"time"
	"unicode/utf8"
)

// CheckStatusAccess is read-only: telemetry never joins a room on the user's behalf.
func (r *Runtime) CheckStatusAccess(ctx context.Context, room, owner string) error {
	if r.Admin == nil || len(r.SurfacePublishers) == 0 {
		return &roomadmin.Error{Status: http.StatusServiceUnavailable}
	}
	members, err := r.Admin.Do(ctx, roomadmin.Request{Action: "members", RoomID: room, Actor: owner})
	if err != nil {
		return err
	}
	for _, m := range members.Members {
		if m.State == "join" && r.SurfacePublishers[m.UserID] {
			return nil
		}
	}
	return ErrForbidden
}

// StatusUpdate is an internal producer API, never a user/LLM command.
type StatusUpdate struct {
	RoomID     string         `json:"room_id"`
	OwnerID    string         `json:"owner_id"`
	ProducerID string         `json:"producer_id"`
	State      map[string]any `json:"state"`
}

func initialStatus(r protocol.Create) (map[string]any, error) {
	started := str(r.Data, "started_at")
	if str(r.Data, "request_id") == "" {
		return nil, ErrInvalid
	}
	if _, err := time.Parse(time.RFC3339Nano, started); err != nil {
		return nil, ErrInvalid
	}
	return map[string]any{"title": r.Title, "owner_id": r.OwnerID, "revision": int64(1), "request_id": str(r.Data, "request_id"), "sequence": int64(1), "status": "running", "current_status": "Pedido recebido", "started_at": started, "updated_at": started, "heartbeat_at": started, "finished_at": nil, "history": []any{}, "active_activities": []any{}, "history_total": 0}, nil
}
func number(m map[string]any, k string) int64 { return revision(map[string]any{"revision": m[k]}) }
func integer(v any) bool {
	switch n := v.(type) {
	case int, int64:
		return true
	case float64:
		return n == math.Trunc(n) && n <= 9007199254740991 && n >= 0
	}
	return false
}
func validateStatus(s map[string]any) error {
	allowed := map[string]bool{}
	for _, k := range []string{"title", "owner_id", "revision", "request_id", "sequence", "status", "current_status", "started_at", "updated_at", "heartbeat_at", "finished_at", "history", "active_activities", "history_total"} {
		allowed[k] = true
	}
	for _, k := range []string{"request_id", "sequence", "status", "current_status", "started_at", "updated_at", "heartbeat_at", "finished_at", "history", "active_activities", "history_total"} {
		if _, ok := s[k]; !ok {
			return ErrInvalid
		}
	}
	for k := range s {
		if !allowed[k] {
			return ErrInvalid
		}
	}
	switch str(s, "status") {
	case "running", "waiting_user", "completed", "failed", "interrupted":
	default:
		return ErrInvalid
	}
	if !integer(s["sequence"]) || number(s, "sequence") < 1 || str(s, "request_id") == "" || utf8.RuneCountInString(str(s, "request_id")) > 160 || utf8.RuneCountInString(str(s, "current_status")) > 200 {
		return ErrInvalid
	}
	for _, k := range []string{"started_at", "updated_at", "heartbeat_at"} {
		if _, err := time.Parse(time.RFC3339Nano, str(s, k)); err != nil {
			return ErrInvalid
		}
	}
	if v := s["finished_at"]; v != nil {
		if _, err := time.Parse(time.RFC3339Nano, str(s, "finished_at")); err != nil {
			return ErrInvalid
		}
	}
	for _, key := range []string{"history", "active_activities"} {
		rows, ok := s[key].([]any)
		if !ok || len(rows) > 200 || (key == "active_activities" && len(rows) > 100) {
			return ErrInvalid
		}
		for _, v := range rows {
			row, ok := v.(map[string]any)
			if !ok || str(row, "id") == "" || utf8.RuneCountInString(str(row, "id")) > 160 || str(row, "label") == "" || utf8.RuneCountInString(str(row, "label")) > 200 {
				return ErrInvalid
			}
			for k := range row {
				if k != "id" && k != "label" && k != "status" && k != "started_at" && k != "finished_at" && k != "duration_ms" {
					return ErrInvalid
				}
			}
			if v := row["finished_at"]; v != nil {
				if _, err := time.Parse(time.RFC3339Nano, str(row, "finished_at")); err != nil {
					return ErrInvalid
				}
			}
			if v := row["duration_ms"]; v != nil {
				n, ok := v.(float64)
				if !ok || n < 0 || math.IsNaN(n) || math.IsInf(n, 0) {
					return ErrInvalid
				}
			}
			switch str(row, "status") {
			case "running", "completed", "failed", "interrupted":
			default:
				return ErrInvalid
			}
			if _, err := time.Parse(time.RFC3339Nano, str(row, "started_at")); err != nil {
				return ErrInvalid
			}
		}
	}
	if !integer(s["history_total"]) || number(s, "history_total") < int64(len(array(s, "history"))) {
		return ErrInvalid
	}
	b, _ := json.Marshal(s)
	if len(b) > 120000 {
		return ErrInvalid
	}
	return nil
}
func (r *Runtime) UpdateStatus(ctx context.Context, id string, u StatusUpdate) (*protocol.Instance, error) {
	if r.Store.Name != "agent_status" || !r.DelegatingAgents[u.ProducerID] {
		return nil, ErrForbidden
	}
	if err := validateStatus(u.State); err != nil {
		return nil, err
	}
	tx, err := r.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	i, err := r.Store.get(ctx, tx, id, true)
	if err != nil {
		return nil, err
	}
	if i.RoomID != u.RoomID || i.OwnerID != u.OwnerID || str(i.State, "request_id") != str(u.State, "request_id") || str(i.State, "started_at") != str(u.State, "started_at") {
		return nil, ErrForbidden
	}
	sequence := number(u.State, "sequence")
	if sequence <= number(i.State, "sequence") {
		if sequence == number(i.State, "sequence") && sequence > 1 {
			want := clone(u.State)
			want["title"] = i.State["title"]
			want["owner_id"] = i.OwnerID
			want["revision"] = i.State["revision"]
			if hash(want) != hash(i.State) {
				return nil, ErrConflict
			}
		}
		tx.Rollback(ctx)
		return i, nil
	}
	state := clone(u.State)
	state["title"] = i.State["title"]
	state["owner_id"] = i.OwnerID
	state["revision"] = revision(i.State) + 1
	b, _ := json.Marshal(state)
	if _, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET state=$2 WHERE id=$1", r.Store.table("instances")), id, b); err != nil {
		return nil, err
	}
	content := map[string]any{"plugin": "agent_status", "instance_id": id, "state": state, "view": View(state)}
	if err = r.Store.insertOp(ctx, tx, "status:"+id+":"+fmt.Sprint(sequence), hash(state), i.RoomID, id, "m.plugin.state.updated", content); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	r.Notify()
	return r.Store.Get(ctx, id)
}

// Direct publication reuses the authenticated journal and stable transaction ID.
// Activity telemetry does not launch one Trama execution per update.
func (r *Runtime) RunStatus(ctx context.Context) {
	for {
		cycle, cancel := context.WithTimeout(ctx, 15*time.Second)
		ids, err := r.Store.StatusPending(cycle)
		progress := false
		if err == nil {
			for _, id := range ids {
				if cycle.Err() != nil {
					break
				}
				attempt, stop := context.WithTimeout(cycle, 2*time.Second)
				_, publishErr := r.call(attempt, "POST", r.RouterURL+"/plugins/agent_status/operations/"+url.PathEscape(id)+"/publish", r.Token, map[string]any{}, nil)
				stop()
				if publishErr != nil {
					var remote *protocol.HTTPError
					if errors.As(publishErr, &remote) && !remote.Retryable {
						op, lookupErr := r.Store.Operation(cycle, id)
						if lookupErr != nil {
							err = lookupErr
							break
						}
						err = r.Store.SuspendStatus(cycle, op.RoomID, fmt.Sprintf("publication returned %d", remote.Status))
						if err == nil {
							slog.Warn("agent status publication suspended", "room_id", op.RoomID, "status", remote.Status)
							// Refresh the batch after suspending the room, so its other
							// already-selected instances are not attempted or logged.
							progress = true
							break
						}
					} else {
						// Log the first failure and powers of two; persist every retry.
						var failures int
						failures, err = r.Store.RetryStatus(cycle, id)
						if err == nil && failures&(failures-1) == 0 {
							slog.Warn("agent status publication pending", "operation_id", id, "failures", failures, "error", publishErr)
						}
					}
					if err != nil {
						break
					}
					continue
				}
				if err = r.Store.Complete(cycle, id); err != nil {
					break
				}
				progress = true
			}
		}
		cancel()
		if err != nil {
			slog.Warn("agent status queue unavailable", "error", err)
		} else if progress {
			// Drain the next ordered snapshots without delaying healthy rooms.
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-r.Wake:
		case <-time.After(r.Store.StatusDelay(ctx)):
		}
	}
}
