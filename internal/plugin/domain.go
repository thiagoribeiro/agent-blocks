// Package plugin implements plugin-owned domain transitions. It is not a workflow engine.
package plugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"strings"
	"time"
)

var ErrInvalid = errors.New("invalid request")
var ErrForbidden = errors.New("forbidden")
var ErrConflict = errors.New("invalid transition")

func clone(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	var n map[string]any
	_ = json.Unmarshal(b, &n)
	return n
}
func str(m map[string]any, k string) string  { v, _ := m[k].(string); return v }
func array(m map[string]any, k string) []any { v, _ := m[k].([]any); return v }
func revision(m map[string]any) int64 {
	switch v := m["revision"].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	return 0
}
func Initial(name string, r protocol.Create) (map[string]any, error) {
	if r.Key == "" || r.RoomID == "" || r.OwnerID == "" || strings.TrimSpace(r.Title) == "" || len(r.Title) > 512 {
		return nil, fmt.Errorf("key, room, owner and title required")
	}
	r.Data = clone(r.Data)
	s := map[string]any{"title": r.Title, "description": r.Description, "owner_id": r.OwnerID, "revision": int64(1)}
	switch name {
	case "monthly_bills":
		bills, err := initialBills(r.Data)
		if err != nil {
			return nil, err
		}
		for k, v := range bills {
			s[k] = v
		}
		s["acl"] = map[string]any{r.OwnerID: "owner"}
	case "agent_status":
		return initialStatus(r)
	case "approval":
		expiry, err := time.Parse(time.RFC3339, r.ExpiresAt)
		if err != nil || !expiry.After(time.Now()) || expiry.After(time.Now().Add(24*time.Hour)) {
			return nil, fmt.Errorf("approval expiry must be within 24 hours")
		}
		s["status"] = "pending"
		s["expires_at"] = r.ExpiresAt
		s["approvers"] = []string{r.OwnerID}
	case "workflow":
		provider := str(r.Data, "provider")
		if provider != "ritesmith" && provider != "trama" {
			return nil, fmt.Errorf("unsupported provider")
		}
		if str(r.Data, "execution_id") == "" {
			return nil, fmt.Errorf("execution_id required")
		}
		s["status"] = "created"
		s["provider"] = provider
		s["execution_id"] = str(r.Data, "execution_id")
		s["steps"] = []any{}
	case "checklist":
		s["acl"] = map[string]any{r.OwnerID: "owner"}
		items := array(r.Data, "items")
		if len(items) == 0 || len(items) > 100 {
			return nil, fmt.Errorf("1..100 items required")
		}
		seen := map[string]bool{}
		for _, raw := range items {
			item, ok := raw.(map[string]any)
			if !ok || str(item, "id") == "" || str(item, "label") == "" || seen[str(item, "id")] {
				return nil, fmt.Errorf("unique item id and label required")
			}
			seen[str(item, "id")] = true
			item["checked"] = false
		}
		s["status"] = "open"
		s["items"] = items
	case "poll":
		opts := array(r.Data, "options")
		if len(opts) < 2 || len(opts) > 20 {
			return nil, fmt.Errorf("2..20 options required")
		}
		seen := map[string]bool{}
		for _, raw := range opts {
			o, ok := raw.(map[string]any)
			if !ok || str(o, "id") == "" || str(o, "label") == "" || seen[str(o, "id")] {
				return nil, fmt.Errorf("unique option id and label required")
			}
			seen[str(o, "id")] = true
			o["votes"] = 0
		}
		s["status"] = "open"
		s["question"] = r.Title
		s["options"] = opts
		s["allow_multiple"] = r.Data["allow_multiple"] == true
		s["votes_by_user"] = map[string]any{}
	case "form_stateful":
		fields := array(r.Data, "fields")
		if len(fields) == 0 || len(fields) > 50 {
			return nil, fmt.Errorf("1..50 fields required")
		}
		seen := map[string]bool{}
		for _, raw := range fields {
			f, ok := raw.(map[string]any)
			if !ok || str(f, "id") == "" || str(f, "label") == "" || seen[str(f, "id")] {
				return nil, fmt.Errorf("unique field id and label required")
			}
			seen[str(f, "id")] = true
			switch str(f, "type") {
			case "text", "number", "boolean", "date":
			case "select":
				for _, option := range array(f, "options") {
					if _, ok := option.(string); !ok {
						return nil, fmt.Errorf("select options must be strings")
					}
				}
				if len(array(f, "options")) == 0 {
					return nil, fmt.Errorf("select options required")
				}
			default:
				return nil, fmt.Errorf("unsupported field type")
			}
		}
		s["status"] = "draft"
		s["fields"] = fields
		s["values"] = map[string]any{}
		s["errors"] = map[string]any{}
	default:
		return nil, fmt.Errorf("unknown plugin")
	}
	return s, nil
}

func Apply(name string, old map[string]any, owner, actor, command string, payload map[string]any, now time.Time) (map[string]any, error) {
	if actor == "" {
		return nil, ErrForbidden
	}
	if old["transfer"] != nil && command != "refresh" && command != "cancel_transfer" {
		return nil, ErrConflict
	}
	if _, hasACL := old["acl"]; hasACL && !CanEdit(old, owner, actor) {
		return nil, ErrForbidden
	}
	if n, handled, err := surfaceCommand(name, old, owner, actor, command, payload, now); handled {
		return n, err
	}
	s := clone(old)
	if command == "refresh" {
		return s, nil
	}
	switch name {
	case "monthly_bills":
		if command == "archive" && actor != owner {
			return nil, ErrForbidden
		}
		if err := applyBills(s, actor, command, payload, now); err != nil {
			return nil, err
		}
	case "agent_status":
		return nil, ErrForbidden
	case "approval":
		if err := applyApproval(s, owner, actor, command, now); err != nil {
			return nil, err
		}
	case "workflow":
		return nil, fmt.Errorf("only refresh is supported; provider owns execution")
	case "checklist":
		if err := applyChecklist(s, old, owner, actor, command, payload, now); err != nil {
			return nil, err
		}
	case "poll":
		if err := applyPoll(s, owner, actor, command, payload); err != nil {
			return nil, err
		}
	case "form_stateful":
		if err := applyForm(s, owner, actor, command, payload, now); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported plugin")
	}
	s["revision"] = revision(old) + 1
	return s, nil
}

func applyApproval(s map[string]any, owner, actor, command string, now time.Time) error {
	status := str(s, "status")
	if actor != owner {
		return ErrForbidden
	}
	if status != "pending" {
		return ErrConflict
	}
	expires, err := time.Parse(time.RFC3339, str(s, "expires_at"))
	if err != nil {
		return err
	}
	if !expires.After(now) {
		s["status"] = "expired"
	} else {
		switch command {
		case "approve":
			s["status"] = "approved"
			s["approved_by"] = actor
		case "reject":
			s["status"] = "rejected"
			s["rejected_by"] = actor
		case "cancel":
			s["status"] = "cancelled"
		default:
			return fmt.Errorf("unsupported command")
		}
		s["resolved_by"] = actor
	}
	s["resolved_at"] = now.UTC().Format(time.RFC3339)
	return nil
}

func applyChecklist(s, old map[string]any, owner, actor, command string, payload map[string]any, now time.Time) error {
	status := str(s, "status")
	if status != "open" && status != "completed" {
		return ErrConflict
	}
	items := array(s, "items")
	switch command {
	case "check_item", "uncheck_item":
		found := false
		for _, raw := range items {
			it := raw.(map[string]any)
			if str(it, "id") == str(payload, "item_id") {
				found = true
				it["checked"] = command == "check_item"
				it["checked_by"] = actor
				it["checked_at"] = now.UTC().Format(time.RFC3339)
			}
		}
		if !found {
			return fmt.Errorf("item not found")
		}
		all := true
		for _, raw := range items {
			if raw.(map[string]any)["checked"] != true {
				all = false
			}
		}
		if all {
			s["status"] = "completed"
		} else {
			s["status"] = "open"
		}
	case "add_item", "remove_item", "reorder_item", "complete", "archive", "cancel":
		if actor != owner && (!CanEdit(old, owner, actor) || command == "archive" || command == "cancel") {
			return ErrForbidden
		}
		switch command {
		case "add_item":
			id, label := str(payload, "item_id"), str(payload, "label")
			if id == "" || label == "" || len(items) >= 100 {
				return fmt.Errorf("invalid item")
			}
			for _, raw := range items {
				if str(raw.(map[string]any), "id") == id {
					return ErrConflict
				}
			}
			items = append(items, map[string]any{"id": id, "label": label, "checked": false})
			s["status"] = "open"
		case "remove_item":
			n := []any{}
			for _, raw := range items {
				if str(raw.(map[string]any), "id") != str(payload, "item_id") {
					n = append(n, raw)
				}
			}
			if len(n) == len(items) || len(n) == 0 {
				return fmt.Errorf("invalid removal")
			}
			items = n
		case "reorder_item":
			order := array(payload, "item_ids")
			if len(order) != len(items) {
				return fmt.Errorf("complete item order required")
			}
			n := []any{}
			seen := map[string]bool{}
			for _, id := range order {
				x, ok := id.(string)
				if !ok || seen[x] {
					return fmt.Errorf("invalid order")
				}
				seen[x] = true
				found := false
				for _, raw := range items {
					if str(raw.(map[string]any), "id") == x {
						n = append(n, raw)
						found = true
					}
				}
				if !found {
					return fmt.Errorf("unknown item")
				}
			}
			items = n
		case "complete":
			for _, raw := range items {
				if raw.(map[string]any)["checked"] != true {
					return ErrConflict
				}
			}
			s["status"] = "completed"
		case "archive":
			s["status"] = "archived"
		case "cancel":
			s["status"] = "cancelled"
		}
	default:
		return fmt.Errorf("unsupported command")
	}
	s["items"] = items
	return nil
}

func applyPoll(s map[string]any, owner, actor, command string, payload map[string]any) error {
	status := str(s, "status")
	if status != "open" {
		return ErrConflict
	}
	switch command {
	case "close", "cancel":
		if actor != owner {
			return ErrForbidden
		}
		if command == "close" {
			s["status"] = "closed"
		} else {
			s["status"] = "cancelled"
		}
	case "vote", "change_vote":
		selections := array(payload, "option_ids")
		if len(selections) == 0 {
			if id := str(payload, "option_id"); id != "" {
				selections = []any{id}
			}
		}
		if len(selections) == 0 || (s["allow_multiple"] != true && len(selections) != 1) {
			return fmt.Errorf("invalid vote")
		}
		allowed := map[string]bool{}
		for _, raw := range array(s, "options") {
			allowed[str(raw.(map[string]any), "id")] = true
		}
		seen := map[string]bool{}
		for _, v := range selections {
			id, ok := v.(string)
			if !ok || !allowed[id] || seen[id] {
				return fmt.Errorf("invalid choice")
			}
			seen[id] = true
		}
		votes := s["votes_by_user"].(map[string]any)
		votes[actor] = selections
		counts := map[string]int{}
		for _, raw := range votes {
			for _, v := range raw.([]any) {
				counts[v.(string)]++
			}
		}
		for _, raw := range array(s, "options") {
			o := raw.(map[string]any)
			o["votes"] = counts[str(o, "id")]
		}
	default:
		return fmt.Errorf("unsupported command")
	}
	return nil
}

func applyForm(s map[string]any, owner, actor, command string, payload map[string]any, now time.Time) error {
	status := str(s, "status")
	if actor != owner {
		return ErrForbidden
	}
	if status == "submitted" || status == "cancelled" {
		return ErrConflict
	}
	switch command {
	case "cancel":
		s["status"] = "cancelled"
	case "save", "submit":
		values, ok := payload["values"].(map[string]any)
		if !ok {
			return fmt.Errorf("values required")
		}
		fields := array(s, "fields")
		allowed := map[string]bool{}
		errs := map[string]any{}
		for _, raw := range fields {
			f := raw.(map[string]any)
			id := str(f, "id")
			allowed[id] = true
			v, present := values[id]
			if !present || v == nil || v == "" {
				if f["required"] == true {
					errs[id] = "Required"
				}
				continue
			}
			valid := true
			switch str(f, "type") {
			case "number":
				_, valid = v.(float64)
			case "boolean":
				_, valid = v.(bool)
			case "text":
				sv, ok := v.(string)
				valid = ok && len(sv) <= 10000
			case "date":
				sv, ok := v.(string)
				_, e := time.Parse("2006-01-02", sv)
				valid = ok && e == nil
			case "select":
				valid = false
				for _, o := range array(f, "options") {
					if sv, ok := v.(string); ok && o == sv {
						valid = true
					}
				}
			}
			if !valid {
				errs[id] = "Invalid value"
			}
		}
		for k := range values {
			if !allowed[k] {
				return fmt.Errorf("unknown field")
			}
		}
		s["values"] = values
		s["errors"] = errs
		s["status"] = "draft"
		if command == "submit" {
			if len(errs) > 0 {
				s["status"] = "invalid"
			} else {
				s["status"] = "submitted"
				s["submitted_by"] = actor
				s["submitted_at"] = now.UTC().Format(time.RFC3339)
			}
		}
	default:
		return fmt.Errorf("unsupported command")
	}
	return nil
}

func View(s map[string]any) map[string]any { return clone(s) }
func Fallback(s map[string]any) string {
	if str(s, "current_status") != "" {
		return fmt.Sprintf("**%s** — %s", str(s, "title"), str(s, "current_status"))
	}
	return fmt.Sprintf("**%s** — %s", str(s, "title"), str(s, "status"))
}
