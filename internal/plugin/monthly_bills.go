package plugin

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

func billGroup(p map[string]any) (any, error) {
	if p["group"] == nil {
		return nil, nil
	}
	group, ok := p["group"].(string)
	if !ok || utf8.RuneCountInString(strings.TrimSpace(group)) > 80 {
		return nil, ErrInvalid
	}
	group = strings.TrimSpace(group)
	if group == "" || group == "Outros" {
		return nil, nil
	}
	return group, nil
}

var monthPattern = regexp.MustCompile(`^\d{4}-(0[1-9]|1[0-2])$`)

func initialBills(data map[string]any) (map[string]any, error) {
	zone := str(data, "timezone")
	if zone == "" {
		zone = "America/Sao_Paulo"
	}
	if _, err := time.LoadLocation(zone); err != nil {
		return nil, ErrInvalid
	}
	return map[string]any{"status": "open", "currency": "BRL", "timezone": zone, "items": []any{}, "periods": map[string]any{}, "selected_period": ""}, nil
}

func amount(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), x >= 0 && x <= 9007199254740991
	case int64:
		return x, x >= 0 && x <= 9007199254740991
	case float64:
		return int64(x), x >= 0 && x <= 9007199254740991 && x == float64(int64(x))
	}
	return 0, false
}

// The due date of a recurrence in a month: its day, or the month's last day when shorter.
func billDueDate(month string, day int, loc *time.Location) (string, error) {
	start, err := time.ParseInLocation("2006-01", month, loc)
	if err != nil || day < 1 || day > 31 {
		return "", ErrInvalid
	}
	if last := time.Date(start.Year(), start.Month()+1, 0, 0, 0, 0, 0, loc).Day(); day > last {
		day = last
	}
	return fmt.Sprintf("%s-%02d", month, day), nil
}

// Periods are snapshots of recurrence descriptions and due dates, so past months and settled
// entries keep their history; only pending entries of the current and later months follow a
// recurrence edit (followRecurrence). Opening an existing month never overwrites a user edit.
// The enclosing instance transaction serializes it.
func materializeBills(s map[string]any, month string) error {
	if !monthPattern.MatchString(month) {
		return ErrInvalid
	}
	periods, ok := s["periods"].(map[string]any)
	if !ok {
		return ErrInvalid
	}
	if existing, exists := periods[month]; exists {
		// A previously empty suggestion can be filled after a historical confirmation.
		// Values already suggested/confirmed or edited by a person are never overwritten.
		period, _ := existing.(map[string]any)
		entries, _ := period["entries"].(map[string]any)
		priorKeys := []string{}
		for k := range periods {
			if k < month {
				priorKeys = append(priorKeys, k)
			}
		}
		sort.Sort(sort.Reverse(sort.StringSlice(priorKeys)))
		for id, raw := range entries {
			entry, _ := raw.(map[string]any)
			if entry["status"] != "pending" || entry["suggested_amount_minor"] != nil || entry["confirmed_amount_minor"] != nil || entry["updated_at"] != nil {
				continue
			}
			for _, key := range priorKeys {
				p, _ := periods[key].(map[string]any)
				es, _ := p["entries"].(map[string]any)
				prev, _ := es[id].(map[string]any)
				if n, valid := amount(prev["confirmed_amount_minor"]); valid {
					entry["suggested_amount_minor"] = n
					entry["suggestion_source_period"] = key
					break
				}
			}
		}
		s["selected_period"] = month
		return nil
	}
	if len(periods) >= 1200 {
		return fmt.Errorf("period limit reached")
	}
	loc, err := time.LoadLocation(str(s, "timezone"))
	if err != nil {
		return ErrInvalid
	}
	entries := map[string]any{}
	keys := []string{}
	for k := range periods {
		if k < month {
			keys = append(keys, k)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	for _, raw := range array(s, "items") {
		it, ok := raw.(map[string]any)
		if !ok || it["active"] == false {
			continue
		}
		id := str(it, "id")
		due, err := billDueDate(month, int(revision(map[string]any{"revision": it["due_day"]})), loc)
		if err != nil {
			return err
		}
		e := map[string]any{"item_id": id, "name": it["name"], "due_date": due, "status": "pending", "suggested_amount_minor": nil, "confirmed_amount_minor": nil, "suggestion_source_period": nil}
		for _, k := range keys {
			prior, _ := periods[k].(map[string]any)
			priorEntries, _ := prior["entries"].(map[string]any)
			p, _ := priorEntries[id].(map[string]any)
			if n, ok := amount(p["confirmed_amount_minor"]); ok {
				e["suggested_amount_minor"] = n
				e["suggestion_source_period"] = k
				break
			}
		}
		entries[id] = e
	}
	periods[month] = map[string]any{"month": month, "entries": entries}
	s["selected_period"] = month
	return nil
}

// A recurrence edit reaches the still pending entries of the current and later months.
// Paid or cancelled entries and past months stay as they were. Entries get no
// updated_at: that marks a person's amount edit, which stops amount suggestions.
func followRecurrence(s map[string]any, it map[string]any, now time.Time) error {
	loc, err := time.LoadLocation(str(s, "timezone"))
	if err != nil {
		return ErrInvalid
	}
	current := now.In(loc).Format("2006-01")
	periods, _ := s["periods"].(map[string]any)
	for month, raw := range periods {
		if month < current {
			continue
		}
		period, _ := raw.(map[string]any)
		entries, _ := period["entries"].(map[string]any)
		entry, ok := entries[str(it, "id")].(map[string]any)
		if !ok || entry["status"] != "pending" {
			continue
		}
		due, err := billDueDate(month, int(revision(map[string]any{"revision": it["due_day"]})), loc)
		if err != nil {
			return err
		}
		entry["name"] = it["name"]
		entry["due_date"] = due
	}
	return nil
}

func applyBills(s map[string]any, actor, command string, p map[string]any, now time.Time) error {
	if str(s, "status") != "open" {
		return ErrConflict
	}
	switch command {
	case "open_period":
		return materializeBills(s, str(p, "period"))
	case "add_bill":
		group, err := billGroup(p)
		if err != nil {
			return err
		}
		id, name := str(p, "item_id"), str(p, "name")
		day, ok := amount(p["due_day"])
		if id == "" || len(id) > 128 || name == "" || len(name) > 512 || !ok || day < 1 || day > 31 || len(array(s, "items")) >= 100 {
			return ErrInvalid
		}
		for _, raw := range array(s, "items") {
			if str(raw.(map[string]any), "id") == id {
				return ErrConflict
			}
		}
		s["items"] = append(array(s, "items"), map[string]any{"id": id, "name": name, "due_day": day, "group": group, "active": true, "created_by": actor, "created_at": now.UTC().Format(time.RFC3339Nano)})
		loc, _ := time.LoadLocation(str(s, "timezone"))
		month := now.In(loc).Format("2006-01")
		periods := s["periods"].(map[string]any)
		if previous, ok := periods[month]; ok {
			scratch := clone(s)
			delete(scratch["periods"].(map[string]any), month)
			if err := materializeBills(scratch, month); err != nil {
				return err
			}
			generated := scratch["periods"].(map[string]any)[month].(map[string]any)["entries"].(map[string]any)
			entries := previous.(map[string]any)["entries"].(map[string]any)
			entries[id] = generated[id]
		} else if err := materializeBills(s, month); err != nil {
			return err
		}
		return nil
	case "update_bill":
		group, err := billGroup(p)
		if err != nil {
			return err
		}
		day, valid := amount(p["due_day"])
		name := str(p, "name")
		if !valid || day < 1 || day > 31 || name == "" || len(name) > 512 {
			return ErrInvalid
		}
		for _, raw := range array(s, "items") {
			it := raw.(map[string]any)
			if str(it, "id") == str(p, "item_id") {
				it["name"] = name
				it["due_day"] = day
				if _, supplied := p["group"]; supplied {
					it["group"] = group
				}
				it["updated_by"] = actor
				it["updated_at"] = now.UTC().Format(time.RFC3339Nano)
				return followRecurrence(s, it, now)
			}
		}
		return ErrInvalid
	case "archive_bill":
		for _, raw := range array(s, "items") {
			it := raw.(map[string]any)
			if str(it, "id") == str(p, "item_id") {
				it["active"] = false
				it["updated_by"] = actor
				it["updated_at"] = now.UTC().Format(time.RFC3339Nano)
				return nil
			}
		}
		return ErrInvalid
	case "confirm_amount", "record_payment", "cancel_entry":
		month := str(p, "period")
		if err := materializeBills(s, month); err != nil {
			return err
		}
		periods := s["periods"].(map[string]any)
		period := periods[month].(map[string]any)
		entries := period["entries"].(map[string]any)
		e, ok := entries[str(p, "item_id")].(map[string]any)
		if !ok {
			return ErrInvalid
		}
		if command == "cancel_entry" {
			if str(e, "status") == "paid" {
				return ErrConflict
			}
			e["status"] = "cancelled"
			e["updated_by"] = actor
			e["updated_at"] = now.UTC().Format(time.RFC3339Nano)
			return nil
		}
		if str(e, "status") != "pending" {
			return ErrConflict
		}
		n, ok := amount(p["amount_minor"])
		if !ok {
			return ErrInvalid
		}
		if command == "record_payment" && p["confirmed"] != true {
			return fmt.Errorf("explicit human confirmation required")
		}
		if len(str(p, "note")) > 2000 {
			return ErrInvalid
		}
		e["confirmed_amount_minor"] = n
		e["updated_by"] = actor
		e["updated_at"] = now.UTC().Format(time.RFC3339Nano)
		if command == "record_payment" {
			paidOn := str(p, "paid_on")
			if paidOn == "" {
				loc, _ := time.LoadLocation(str(s, "timezone"))
				paidOn = now.In(loc).Format("2006-01-02")
			}
			if _, err := time.Parse("2006-01-02", paidOn); err != nil {
				return ErrInvalid
			}
			e["paid_on"] = paidOn
			e["status"] = "paid"
			e["paid_amount_minor"] = n
			e["paid_by"] = actor
			e["paid_at"] = now.UTC().Format(time.RFC3339Nano)
			e["note"] = str(p, "note")
		}
		return nil
	case "archive":
		s["status"] = "archived"
		return nil
	}
	return ErrInvalid
}
