package plugin

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

func CanEdit(state map[string]any, owner, actor string) bool {
	if actor == owner {
		return true
	}
	acl, _ := state["acl"].(map[string]any)
	return acl[actor] == "editor"
}
func transferable(name string) bool { return name == "checklist" || name == "monthly_bills" }
func editable(name string) bool {
	return name == "checklist" || name == "poll" || name == "form_stateful" || name == "monthly_bills"
}
func surfaceCommand(name string, old map[string]any, owner, actor, command string, p map[string]any, now time.Time) (map[string]any, bool, error) {
	if command != "rename" && command != "preview_share" && command != "share" && command != "invite_participant" && command != "remove_participant" && command != "cancel_transfer" {
		return nil, false, nil
	}
	if !editable(name) || !CanEdit(old, owner, actor) {
		return nil, true, ErrForbidden
	}
	s := clone(old)
	switch command {
	case "cancel_transfer":
		if actor != owner {
			return nil, true, ErrForbidden
		}
		t, _ := s["transfer"].(map[string]any)
		if t == nil || str(t, "phase") != "preparing" {
			return nil, true, ErrConflict
		}
		delete(s, "transfer")
	case "rename":
		title := strings.TrimSpace(str(p, "title"))
		if title == "" || utf8.RuneCountInString(title) > 120 || strings.ContainsAny(title, "\n\r\x00") {
			return nil, true, ErrInvalid
		}
		s["title"] = title
	case "preview_share":
		if actor != owner || !transferable(name) || s["shared"] != nil || s["transfer"] != nil {
			return nil, true, ErrForbidden
		}
		// Only Store.handlePrepared supplies this data, after administrative validation.
		preview, ok := p["_validated_preview"].(map[string]any)
		if !ok {
			return nil, true, ErrInvalid
		}
		s["sharing_preview"] = clone(preview)
	case "share":
		if actor != owner || !transferable(name) || s["shared"] != nil || s["transfer"] != nil {
			return nil, true, ErrForbidden
		}
		preview, _ := s["sharing_preview"].(map[string]any)
		expiry, err := time.Parse(time.RFC3339Nano, str(preview, "expires_at"))
		if str(p, "preview_token") == "" || str(p, "preview_token") != str(preview, "token") || err != nil || !expiry.After(now) {
			return nil, true, fmt.Errorf("Review the participants before sharing.")
		}
		invitees := array(preview, "invitees")
		if len(invitees) > 100 {
			return nil, true, ErrInvalid
		}
		for _, v := range invitees {
			if u, ok := v.(string); !ok || u == "" || len(u) > 128 {
				return nil, true, ErrInvalid
			}
		}
		delete(s, "sharing_error")
		s["transfer"] = map[string]any{"phase": "preparing", "invitees": invitees}
	case "invite_participant", "remove_participant":
		if actor != owner || s["shared"] == nil {
			return nil, true, ErrForbidden
		}
		if command == "remove_participant" && str(p, "user_id") == owner {
			return nil, true, ErrForbidden
		}
		if s["access_operation"] != nil {
			return nil, true, ErrConflict
		}
		if command == "invite_participant" && str(p, "username") == "" {
			return nil, true, ErrInvalid
		}
		if command == "remove_participant" && str(p, "user_id") == "" {
			return nil, true, ErrInvalid
		}
		s["access_operation"] = map[string]any{"command": command, "username": str(p, "username"), "user_id": str(p, "user_id")}
		// Deny immediately, even if the transport removal must be retried.
		if command == "remove_participant" {
			acl, _ := s["acl"].(map[string]any)
			delete(acl, str(p, "user_id"))
		}
	}
	s["revision"] = revision(old) + 1
	s["updated_at"] = now.UTC().Format(time.RFC3339Nano)
	return s, true, nil
}
