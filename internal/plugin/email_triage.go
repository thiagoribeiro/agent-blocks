package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"time"
	"unicode/utf8"
)

// email_triage holds the owner's pending decisions about their mailbox triage.
//
// The view (important mail, recent mail, cleanup groups and batches) is owned by
// a producer — the agent that reads the mailbox — and replaced through the
// producer API. The plugin has no mail logic: it validates and journals human
// decisions ("this group is mine", "not important", "undo this batch"), and
// the producer reports each one as applied or failed. Decisions are never
// delegable: an LLM cannot issue them on the owner's behalf.

const (
	emailViewMaxBytes  = 256 << 10
	emailDecisionsMax  = 200
	emailResolvedKeep  = 50
	emailStringMax     = 500
	emailArrayMax      = 200
	emailDepthMax      = 6
	emailIDMax         = 160
	emailErrorMax      = 300
	emailPendingMax    = 50
	emailDecisionStale = 7 * 24 * time.Hour
)

// Which view list holds the target of each command, and the payload key naming it.
var emailCommands = map[string]struct {
	key   string
	lists []string
}{
	"mark_done":      {"item_id", []string{"important"}},
	"not_important":  {"item_id", []string{"important"}},
	"should_alert":   {"item_id", []string{"recent", "important"}},
	"group_mine":     {"group_id", []string{"cleanup.review", "cleanup.wrong"}},
	"group_not_mine": {"group_id", []string{"cleanup.review"}},
	"group_unsure":   {"group_id", []string{"cleanup.review"}},
	"undo_batch":     {"batch_id", []string{"cleanup.batches"}},
}

var emailViewKeys = map[string]bool{"important": true, "recent": true, "cleanup": true, "updated_at": true, "notice": true}

// boundedJSON rejects deep, wide or long values without interpreting them.
func boundedJSON(v any, depth int) error {
	if depth > emailDepthMax {
		return ErrInvalid
	}
	switch x := v.(type) {
	case map[string]any:
		if len(x) > 50 {
			return ErrInvalid
		}
		for k, e := range x {
			if utf8.RuneCountInString(k) > 64 {
				return ErrInvalid
			}
			if err := boundedJSON(e, depth+1); err != nil {
				return err
			}
		}
	case []any:
		if len(x) > emailArrayMax {
			return ErrInvalid
		}
		for _, e := range x {
			if err := boundedJSON(e, depth+1); err != nil {
				return err
			}
		}
	case string:
		if utf8.RuneCountInString(x) > emailStringMax {
			return ErrInvalid
		}
	case float64, bool, nil, int, int64:
	default:
		return ErrInvalid
	}
	return nil
}

// emailList returns a view list by dotted path ("cleanup.review").
func emailList(view map[string]any, path string) []any {
	switch path {
	case "important", "recent":
		return array(view, path)
	}
	cleanup, _ := view["cleanup"].(map[string]any)
	switch path {
	case "cleanup.review":
		return array(cleanup, "review")
	case "cleanup.wrong":
		return array(cleanup, "wrong")
	case "cleanup.batches":
		return array(cleanup, "batches")
	}
	return nil
}

func validateEmailView(view map[string]any) error {
	if view == nil {
		return ErrInvalid
	}
	for k := range view {
		if !emailViewKeys[k] {
			return ErrInvalid
		}
	}
	if err := boundedJSON(view, 0); err != nil {
		return err
	}
	if b, _ := json.Marshal(view); len(b) > emailViewMaxBytes {
		return ErrInvalid
	}
	if c, ok := view["cleanup"]; ok {
		if _, ok := c.(map[string]any); !ok {
			return ErrInvalid
		}
	}
	for _, path := range []string{"important", "recent", "cleanup.review", "cleanup.wrong", "cleanup.batches"} {
		seen := map[string]bool{}
		for _, raw := range emailList(view, path) {
			row, ok := raw.(map[string]any)
			id := str(row, "id")
			if !ok || id == "" || utf8.RuneCountInString(id) > emailIDMax || seen[id] {
				return ErrInvalid
			}
			seen[id] = true
		}
	}
	return nil
}

func initialEmailTriage(r protocol.Create) (map[string]any, error) {
	view, _ := r.Data["view"].(map[string]any)
	if view == nil {
		view = map[string]any{}
	}
	if err := validateEmailView(view); err != nil {
		return nil, fmt.Errorf("invalid view")
	}
	return map[string]any{"title": r.Title, "description": r.Description, "owner_id": r.OwnerID, "revision": int64(1),
		"status": "open", "producer_sequence": int64(0), "view": view, "decisions": []any{}}, nil
}

func emailTargetExists(view map[string]any, lists []string, id string) bool {
	for _, path := range lists {
		for _, raw := range emailList(view, path) {
			if row, ok := raw.(map[string]any); ok && str(row, "id") == id {
				return true
			}
		}
	}
	return false
}

// applyEmailTriage records a human decision; only the owner decides, the
// producer carries it out. The target must be in the current view.
func applyEmailTriage(s map[string]any, owner, actor, command string, p map[string]any, now time.Time) error {
	if actor != owner {
		return ErrForbidden
	}
	spec, ok := emailCommands[command]
	if !ok {
		return fmt.Errorf("unsupported command")
	}
	target := str(p, spec.key)
	if target == "" || utf8.RuneCountInString(target) > emailIDMax {
		return ErrInvalid
	}
	for k := range p {
		if k != spec.key && k != "confirmed" {
			return ErrInvalid
		}
	}
	if command == "undo_batch" && p["confirmed"] != true {
		return fmt.Errorf("undo requires confirmation")
	}
	view, _ := s["view"].(map[string]any)
	if !emailTargetExists(view, spec.lists, target) {
		return fmt.Errorf("target no longer in view: refresh before deciding")
	}
	decisions := array(s, "decisions")
	pending := 0
	for _, raw := range decisions {
		d := raw.(map[string]any)
		if str(d, "state") != "pending" {
			continue
		}
		pending++
		if str(d, "target") == target {
			return fmt.Errorf("a decision about this is already pending")
		}
	}
	if pending >= emailPendingMax {
		return fmt.Errorf("too many pending decisions: wait for the mailbox to catch up")
	}
	decisions = append(decisions, map[string]any{"id": fmt.Sprintf("d%d", revision(s)+1), "command": command,
		"target": target, "actor": actor, "created_at": now.UTC().Format(time.RFC3339Nano), "state": "pending"})
	s["decisions"] = trimEmailDecisions(decisions)
	return nil
}

// trimEmailDecisions keeps every pending decision and the latest resolved ones.
func trimEmailDecisions(all []any) []any {
	resolved := 0
	for _, raw := range all {
		if str(raw.(map[string]any), "state") != "pending" {
			resolved++
		}
	}
	drop := resolved - emailResolvedKeep
	out := []any{}
	for _, raw := range all {
		if drop > 0 && str(raw.(map[string]any), "state") != "pending" {
			drop--
			continue
		}
		out = append(out, raw)
	}
	if len(out) > emailDecisionsMax {
		out = out[len(out)-emailDecisionsMax:]
	}
	return out
}

// EmailUpdate is the producer API: a new view and/or decision outcomes. Never a user/LLM command.
type EmailUpdate struct {
	RoomID     string           `json:"room_id"`
	OwnerID    string           `json:"owner_id"`
	ProducerID string           `json:"producer_id"`
	Sequence   int64            `json:"sequence"`
	View       map[string]any   `json:"view,omitempty"`
	Decisions  []map[string]any `json:"decisions,omitempty"`
}

// resolveEmail applies a producer update to a copy of the state.
func resolveEmail(old map[string]any, u EmailUpdate, now time.Time) (map[string]any, error) {
	s := clone(old)
	if u.View != nil {
		if err := validateEmailView(u.View); err != nil {
			return nil, err
		}
		s["view"] = clone(u.View)
	}
	if len(u.Decisions) > emailDecisionsMax {
		return nil, ErrInvalid
	}
	decisions := array(s, "decisions")
	byID := map[string]map[string]any{}
	for _, raw := range decisions {
		d := raw.(map[string]any)
		byID[str(d, "id")] = d
	}
	for _, r := range u.Decisions {
		for k := range r {
			if k != "id" && k != "state" && k != "error" {
				return nil, ErrInvalid
			}
		}
		d, ok := byID[str(r, "id")]
		state := str(r, "state")
		if !ok || (state != "applied" && state != "failed") || utf8.RuneCountInString(str(r, "error")) > emailErrorMax {
			return nil, ErrInvalid
		}
		if str(d, "state") != "pending" {
			if str(d, "state") != state {
				return nil, ErrConflict
			}
			continue // replay
		}
		d["state"] = state
		d["resolved_at"] = now.UTC().Format(time.RFC3339Nano)
		if state == "failed" {
			d["error"] = str(r, "error")
		}
	}
	// A decision the producer never picks up must not stay pending forever.
	for _, raw := range decisions {
		d := raw.(map[string]any)
		created, err := time.Parse(time.RFC3339Nano, str(d, "created_at"))
		if str(d, "state") == "pending" && err == nil && now.Sub(created) > emailDecisionStale {
			d["state"], d["error"], d["resolved_at"] = "failed", "expired", now.UTC().Format(time.RFC3339Nano)
		}
	}
	s["decisions"] = trimEmailDecisions(decisions)
	s["producer_sequence"] = u.Sequence
	s["revision"] = revision(old) + 1
	return s, nil
}

func (r *Runtime) UpdateEmailTriage(ctx context.Context, id string, u EmailUpdate) (*protocol.Instance, error) {
	if r.Store.Name != "email_triage" || !r.Producers[u.ProducerID] {
		return nil, ErrForbidden
	}
	if u.Sequence < 1 || (u.View == nil && len(u.Decisions) == 0) {
		return nil, ErrInvalid
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
	if i.RoomID != u.RoomID || i.OwnerID != u.OwnerID {
		return nil, ErrForbidden
	}
	opID := "producer:" + id + ":" + fmt.Sprint(u.Sequence)
	if u.Sequence <= number(i.State, "producer_sequence") {
		// The same sequence twice is a lost response: same input, same result.
		var prior string
		err = tx.QueryRow(ctx, fmt.Sprintf("SELECT input_hash FROM %s WHERE id=$1", r.Store.table("operations")), opID).Scan(&prior)
		if err == nil && prior != hash(u) {
			return nil, ErrConflict
		}
		tx.Rollback(ctx)
		return i, nil
	}
	state, err := resolveEmail(i.State, u, time.Now())
	if err != nil {
		return nil, err
	}
	b, _ := json.Marshal(state)
	if _, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET state=$2 WHERE id=$1", r.Store.table("instances")), id, b); err != nil {
		return nil, err
	}
	content := map[string]any{"plugin": "email_triage", "instance_id": id, "state": state, "view": View(state)}
	if err = r.Store.insertOp(ctx, tx, opID, hash(u), i.RoomID, id, "m.plugin.state.updated", content); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	r.Notify()
	return r.Store.Get(ctx, id)
}
