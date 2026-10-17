package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Called on authenticated internal reads. It materializes only the current month,
// and never selects it over a user's existing period choice.
func (s *Store) CurrentBills(ctx context.Context, id string) error {
	if s.Name != "monthly_bills" {
		return nil
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	i, err := s.get(ctx, tx, id, true)
	if err != nil {
		return err
	}
	if i.State["transfer"] != nil || str(i.State, "status") != "open" {
		return nil
	}
	loc, err := time.LoadLocation(str(i.State, "timezone"))
	if err != nil {
		return err
	}
	month := time.Now().In(loc).Format("2006-01")
	periods, _ := i.State["periods"].(map[string]any)
	if _, exists := periods[month]; exists {
		return nil
	}
	selected := str(i.State, "selected_period")
	if err = materializeBills(i.State, month); err != nil {
		return err
	}
	if selected != "" {
		i.State["selected_period"] = selected
	}
	i.State["revision"] = revision(i.State) + 1
	b, _ := json.Marshal(i.State)
	if _, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET state=$2 WHERE id=$1", s.table("instances")), id, b); err != nil {
		return err
	}
	content := map[string]any{"plugin": s.Name, "instance_id": id, "state": i.State, "view": View(i.State)}
	if err = s.insertOp(ctx, tx, "month:"+id+":"+month, hash(content), i.RoomID, id, "m.plugin.state.updated", content); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
