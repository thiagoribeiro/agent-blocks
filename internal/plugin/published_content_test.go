package plugin

import (
	"encoding/json"
	"testing"
)

func TestStateUpdatesPublishTheStateOnce(t *testing.T) {
	state := map[string]any{"title": "Lista", "revision": int64(2), "items": []any{"a"}}
	c := publishedContent("m.plugin.state.updated", map[string]any{"plugin": "checklist", "instance_id": "i", "state": state, "view": View(state), "command_event_id": "e"})
	if _, ok := c["view"]; ok || c["state"] == nil || c["command_event_id"] != "e" {
		t.Fatal("duplicate view kept or fields lost", c)
	}
	b, _ := json.Marshal(c)
	full, _ := json.Marshal(map[string]any{"plugin": "checklist", "instance_id": "i", "state": state, "view": View(state), "command_event_id": "e"})
	one, _ := json.Marshal(state)
	if len(full)-len(b) < len(one) {
		t.Fatal("the copy was not removed", len(b), len(full))
	}
	// creation keeps both (BRIAN requires both there)
	if c := publishedContent("m.plugin.instance.created", map[string]any{"state": state, "view": View(state)}); c["view"] == nil {
		t.Fatal("creation lost its view")
	}
	// a view that is not the state is kept
	other := map[string]any{"title": "x"}
	if c := publishedContent("m.plugin.state.updated", map[string]any{"state": state, "view": other}); c["view"] == nil {
		t.Fatal("distinct view dropped")
	}
	// rejections and messages are untouched
	r := map[string]any{"plugin": "p", "reason": "x"}
	if c := publishedContent("m.plugin.command.rejected", r); len(c) != 2 {
		t.Fatal(c)
	}
}
