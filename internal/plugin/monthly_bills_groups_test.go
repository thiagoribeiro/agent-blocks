package plugin

import (
	"strings"
	"testing"
	"time"
)

func TestBillGroupsPreservePaymentsAndLegacyEdits(t *testing.T) {
	s, err := Initial("monthly_bills", request(map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	apply := func(command string, payload map[string]any) {
		t.Helper()
		s, err = Apply("monthly_bills", s, "owner", "owner", command, payload, now)
		if err != nil {
			t.Fatal(err)
		}
	}
	apply("add_bill", map[string]any{"item_id": "water", "name": "Água", "due_day": 10, "group": " Lake house "})
	if array(s, "items")[0].(map[string]any)["group"] != "Lake house" {
		t.Fatal("group not normalized")
	}
	apply("record_payment", map[string]any{"period": "2026-10", "item_id": "water", "amount_minor": 12000, "confirmed": true})
	before := hash(s["periods"])
	apply("update_bill", map[string]any{"item_id": "water", "name": "Água", "due_day": 10, "group": "Chalé"})
	if hash(s["periods"]) != before {
		t.Fatal("group changed historical entries")
	}
	apply("update_bill", map[string]any{"item_id": "water", "name": "Água atualizada", "due_day": 11})
	if array(s, "items")[0].(map[string]any)["group"] != "Chalé" {
		t.Fatal("legacy edit cleared group")
	}
	exported := exportState("monthly_bills", s)
	if array(exported, "items")[0].(map[string]any)["group"] != "Chalé" {
		t.Fatal("shared export lost group")
	}
	apply("update_bill", map[string]any{"item_id": "water", "name": "Água", "due_day": 10, "group": nil})
	if array(s, "items")[0].(map[string]any)["group"] != nil {
		t.Fatal("clear group failed")
	}
}
func TestBillGroupRejectsInvalidWithoutMutation(t *testing.T) {
	s, _ := Initial("monthly_bills", request(map[string]any{}))
	for _, group := range []any{42, []string{"Hugo"}, strings.Repeat("á", 81)} {
		before := hash(s)
		_, err := Apply("monthly_bills", s, "owner", "owner", "add_bill", map[string]any{"item_id": "a", "name": "A", "due_day": 10, "group": group}, time.Now())
		if err == nil || hash(s) != before {
			t.Fatal("invalid group accepted or mutated state", group)
		}
	}
}
