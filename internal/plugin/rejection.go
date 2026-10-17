package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
)

// Invalid creation/disabled capabilities must not poison the sync cursor. Persist the
// definitive rejection just like any other plugin transition, without an instance.
func (s *Store) RejectEvent(ctx context.Context, e protocol.Event, reason string) (*protocol.Operation, error) {
	h, err := eventHash(e)
	if err != nil {
		return nil, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var c protocol.Command
	if err = json.Unmarshal(e.Content, &c); err != nil {
		return nil, err
	}
	content := map[string]any{"plugin": s.Name, "instance_id": c.InstanceID, "command_event_id": e.EventID, "reason": reason}
	if err = s.insertOp(ctx, tx, e.EventID, h, e.RoomID, c.InstanceID, "m.plugin.command.rejected", content); err != nil {
		return nil, err
	}
	var prior string
	if err = tx.QueryRow(ctx, fmt.Sprintf("SELECT input_hash FROM %s WHERE id=$1", s.table("operations")), e.EventID).Scan(&prior); err != nil {
		return nil, err
	}
	if prior != h {
		return nil, ErrConflict
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET done=true WHERE id=$1", s.table("shared_creations")), e.EventID); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.Operation(ctx, e.EventID)
}
