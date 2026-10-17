package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/thiagoribeiro/agent-blocks/internal/roomadmin"
)

func (s *Store) SyncMembers(ctx context.Context, id string, members []roomadmin.Member) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	i, err := s.get(ctx, tx, id, true)
	if err != nil {
		return err
	}
	shared, _ := i.State["shared"].(map[string]any)
	if shared == nil || i.State["transfer"] != nil {
		return nil
	}
	before := hash(i.State)
	states := map[string]string{}
	for _, m := range members {
		states[m.UserID] = m.State
	}
	acl, _ := i.State["acl"].(map[string]any)
	for _, raw := range array(shared, "participants") {
		person := raw.(map[string]any)
		id := str(person, "user_id")
		state := states[id]
		if state == "" {
			state = "leave"
		}
		person["state"] = state
		if (state == "leave" || state == "ban") && id != i.OwnerID {
			delete(acl, id)
		}
	}
	if hash(i.State) == before {
		return nil
	}
	i.State["revision"] = revision(i.State) + 1
	data, _ := json.Marshal(i.State)
	if _, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET state=$2 WHERE id=$1", s.table("instances")), id, data); err != nil {
		return err
	}
	content := map[string]any{"plugin": s.Name, "instance_id": id, "state": i.State, "view": View(i.State)}
	if err = s.insertOp(ctx, tx, fmt.Sprintf("membership:%s:%d", id, revision(i.State)), hash(content), i.RoomID, id, "m.plugin.state.updated", content); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
