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

func TestBillEditFollowsPendingCurrentAndLaterMonths(t *testing.T) {
	s, err := Initial("monthly_bills", request(map[string]any{}))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	apply := func(command string, payload map[string]any) {
		t.Helper()
		s, err = Apply("monthly_bills", s, "owner", "owner", command, payload, now)
		if err != nil {
			t.Fatal(command, err)
		}
	}
	entry := func(month, id string) map[string]any {
		t.Helper()
		return s["periods"].(map[string]any)[month].(map[string]any)["entries"].(map[string]any)[id].(map[string]any)
	}
	apply("add_bill", map[string]any{"item_id": "net", "name": "Internet", "due_day": 10})
	apply("add_bill", map[string]any{"item_id": "power", "name": "Luz", "due_day": 5})
	apply("add_bill", map[string]any{"item_id": "rent", "name": "Aluguel", "due_day": 1})
	apply("open_period", map[string]any{"period": "2026-09"})
	apply("open_period", map[string]any{"period": "2026-11"})
	apply("record_payment", map[string]any{"period": "2026-10", "item_id": "power", "amount_minor": 9000, "confirmed": true})
	apply("confirm_amount", map[string]any{"period": "2026-10", "item_id": "rent", "amount_minor": 150000})
	paid := hash(entry("2026-10", "power"))
	past := hash(s["periods"].(map[string]any)["2026-09"])

	apply("update_bill", map[string]any{"item_id": "net", "name": "Internet Vivo", "due_day": 31})
	for month, due := range map[string]string{"2026-10": "2026-10-31", "2026-11": "2026-11-30"} {
		if e := entry(month, "net"); e["name"] != "Internet Vivo" || e["due_date"] != due || e["updated_at"] != nil {
			t.Fatalf("%s pending entry did not follow the recurrence: %v", month, e)
		}
	}
	apply("update_bill", map[string]any{"item_id": "power", "name": "Energia", "due_day": 5})
	apply("update_bill", map[string]any{"item_id": "rent", "name": "Aluguel apto", "due_day": 1})
	if hash(entry("2026-10", "power")) != paid {
		t.Fatal("paid entry changed")
	}
	if entry("2026-11", "power")["name"] != "Energia" {
		t.Fatal("later pending entry of a paid bill did not follow")
	}
	if hash(s["periods"].(map[string]any)["2026-09"]) != past {
		t.Fatal("past month changed")
	}
	if e := entry("2026-10", "rent"); e["name"] != "Aluguel apto" {
		t.Fatalf("pending entry with a confirmed amount did not follow: %v", e)
	} else if n, _ := amount(e["confirmed_amount_minor"]); n != 150000 {
		t.Fatalf("pending entry with a confirmed amount: %v", e)
	}
}

func TestBillDueDateClampsToMonthEnd(t *testing.T) {
	for month, want := range map[string]string{"2026-02": "2026-02-28", "2028-02": "2028-02-29", "2026-04": "2026-04-30", "2026-01": "2026-01-31"} {
		if got, err := billDueDate(month, 31, time.UTC); err != nil || got != want {
			t.Fatal(month, got, err)
		}
	}
	if _, err := billDueDate("2026-02", 0, time.UTC); err == nil {
		t.Fatal("day 0 accepted")
	}
}
