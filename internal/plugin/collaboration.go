package plugin

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"github.com/thiagoribeiro/agent-blocks/internal/roomadmin"
	"log/slog"
	"strings"
	"time"
)

func sharingUUID(key string) string {
	h := sha256.Sum256([]byte(key))
	h[6] = (h[6] & 15) | 0x50
	h[8] = (h[8] & 63) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", h[:4], h[4:6], h[6:8], h[8:10], h[10:16])
}
func (s *Store) migrateSharing(ctx context.Context) error {
	_, err := s.Pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (operation_id text PRIMARY KEY,source_id text NOT NULL,target_id text NOT NULL UNIQUE,source_room text NOT NULL,target_room text NOT NULL,phase text NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
 CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY,input_hash text NOT NULL,event jsonb NOT NULL,done boolean NOT NULL DEFAULT false);`, s.table("transfers"), s.table("shared_creations")))
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY, participants jsonb NOT NULL)", s.table("sharing_participants")))
	return err
}
func sharedMetadata(key string, p *roomadmin.Prepared) map[string]any {
	members := []any{}
	for _, m := range p.Members {
		members = append(members, map[string]any{"user_id": m.UserID, "username": m.Username, "state": m.State})
	}
	return map[string]any{"id": key, "room_id": p.RoomID, "participants": members}
}
func sharedACL(owner string, p *roomadmin.Prepared) map[string]any {
	acl := map[string]any{owner: "owner"}
	for _, m := range p.Members {
		if m.UserID != owner {
			acl[m.UserID] = "editor"
		}
	}
	return acl
}
func (r *Runtime) prepareShared(ctx context.Context, room, actor, key string, invitees []any) (*roomadmin.Prepared, error) {
	if r.Admin == nil {
		return nil, fmt.Errorf("sharing is disabled")
	}
	members, err := r.snapshotParticipants(ctx, room, actor, key, invitees)
	if err != nil {
		return nil, err
	}
	out, err := r.Admin.Do(ctx, roomadmin.Request{Action: "prepare", RoomID: room, Actor: actor, Key: key, Participants: members})
	if err != nil {
		return nil, err
	}
	if out.Prepared == nil || out.Prepared.RoomID == "" {
		return nil, ErrInvalid
	}
	return out.Prepared, nil
}

// The selection is durable before provisioning, so retries cannot mirror later
// changes in the origin. The adapter receives namespace-scoped user identities.
func (r *Runtime) snapshotParticipants(ctx context.Context, room, actor, key string, invitees []any) ([]roomadmin.Member, error) {
	var data []byte
	err := r.Store.Pool.QueryRow(ctx, fmt.Sprintf("SELECT participants FROM %s WHERE id=$1", r.Store.table("sharing_participants")), key).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		if r.Admin == nil {
			return nil, fmt.Errorf("room admin unavailable")
		}
		names := []string{}
		for _, raw := range invitees {
			name, ok := raw.(string)
			if !ok {
				return nil, ErrInvalid
			}
			names = append(names, name)
		}
		selected, err := r.Admin.Do(ctx, roomadmin.Request{Action: "validate", RoomID: room, Actor: actor, Invitees: names})
		if err != nil {
			return nil, err
		}
		data, _ = json.Marshal(selected.Members)
		if _, err = r.Store.Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s(id,participants) VALUES($1,$2) ON CONFLICT DO NOTHING", r.Store.table("sharing_participants")), key, data); err != nil {
			return nil, err
		}
		err = r.Store.Pool.QueryRow(ctx, fmt.Sprintf("SELECT participants FROM %s WHERE id=$1", r.Store.table("sharing_participants")), key).Scan(&data)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	members := []roomadmin.Member{}
	if err = json.Unmarshal(data, &members); err != nil {
		return nil, err
	}
	return members, nil
}

func blockReference(plugin, id, room, title string) map[string]any {
	return map[string]any{"type": "reference", "title": title, "kind": "surface", "room_id": room, "metadata": map[string]any{"plugin": plugin, "instance_id": id}}
}
func referenceMessage(ref map[string]any) map[string]any {
	return map[string]any{"schema": "brian.message", "version": "1.0", "body": []any{ref}}
}

// Freeze commits before external work. The imported instance is inserted in the SAME
// transaction that permanently fences the source; a crash cannot leave two writable copies.
func (r *Runtime) Transfer(ctx context.Context, id string) error {
	s := r.Store
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	i, err := s.get(ctx, tx, id, true)
	if err != nil {
		return err
	}
	t, _ := i.State["transfer"].(map[string]any)
	if t == nil || str(t, "phase") == "completed" || str(t, "phase") == "moved" {
		return nil
	}
	if str(t, "phase") == "failed" {
		return nil
	}
	if !transferable(s.Name) {
		return ErrForbidden
	}
	if str(t, "phase") == "preparing" {
		t["phase"] = "frozen"
		i.State["revision"] = revision(i.State) + 1
		b, _ := json.Marshal(i.State)
		if _, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET state=$2 WHERE id=$1", s.table("instances")), id, b); err != nil {
			return err
		}
		if err = s.insertOp(ctx, tx, "freeze:"+str(t, "operation_id"), hash(i.State), i.RoomID, id, "m.plugin.state.updated", map[string]any{"plugin": s.Name, "instance_id": id, "state": i.State, "view": View(i.State)}); err != nil {
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	createdAt, err := r.originalCreationTime(ctx, i)
	if err != nil {
		return err
	}
	key := sharingUUID(s.Name + ":" + str(t, "operation_id"))
	p, err := r.prepareShared(ctx, i.RoomID, i.OwnerID, key, array(t, "invitees"))
	if err != nil {
		var permanent *roomadmin.Error
		if errors.As(err, &permanent) && (permanent.Status == 400 || permanent.Status == 403) {
			return s.abortTransfer(ctx, id)
		}
		return err
	}
	tx, err = s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	i, err = s.get(ctx, tx, id, true)
	if err != nil {
		return err
	}
	t, _ = i.State["transfer"].(map[string]any)
	if t == nil || str(t, "phase") == "moved" || str(t, "phase") == "completed" {
		return nil
	}
	if str(t, "phase") != "frozen" {
		return ErrConflict
	}
	operation := str(t, "operation_id")
	destID := hash([]string{s.Name, p.RoomID, i.OwnerID, operation})[:32]
	if createdAt != "" {
		i.State["created_at"] = createdAt
	} else {
		delete(i.State, "created_at")
	}
	dest := exportState(s.Name, i.State)
	delete(dest, "transfer")
	delete(dest, "access_operation")
	dest["shared"] = sharedMetadata(key, p)
	dest["acl"] = sharedACL(i.OwnerID, p)
	dest["revision"] = int64(1)
	// Only safe provenance: never export private room IDs/event IDs or messages into destination.
	dest["updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	req := protocol.Create{Key: operation, RoomID: p.RoomID, OwnerID: i.OwnerID, Title: str(dest, "title"), Data: map[string]any{}}
	request, _ := json.Marshal(req)
	state, _ := json.Marshal(dest)
	if _, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id,create_key,room_id,owner_id,source_pos,request,state) VALUES($1,$1,$2,$3,0,$4,$5) ON CONFLICT DO NOTHING", s.table("instances")), destID, p.RoomID, i.OwnerID, request, state); err != nil {
		return err
	}
	t["phase"] = "moved"
	t["room_id"] = p.RoomID
	t["instance_id"] = destID
	i.State["revision"] = revision(i.State) + 1
	b, _ := json.Marshal(i.State)
	if _, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET state=$2 WHERE id=$1", s.table("instances")), id, b); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s(operation_id,source_id,target_id,source_room,target_room,phase) VALUES($1,$2,$3,$4,$5,'committed') ON CONFLICT DO NOTHING", s.table("transfers")), operation, id, destID, i.RoomID, p.RoomID); err != nil {
		return err
	}
	for _, op := range []struct {
		id, room, instance, kind string
		content                  map[string]any
	}{
		{"create:" + destID, p.RoomID, destID, "m.plugin.instance.created", map[string]any{"plugin": s.Name, "instance_id": destID, "state": dest, "view": View(dest)}},
		{"moved:" + operation, i.RoomID, id, "m.plugin.state.updated", map[string]any{"plugin": s.Name, "instance_id": id, "state": i.State, "view": View(i.State)}},
		{"redirect:" + operation, i.RoomID, id, "m.ui.message", referenceMessage(blockReference(s.Name, destID, p.RoomID, "Open shared surface"))},
	} {
		if err = s.insertOp(ctx, tx, op.id, hash(op.content), op.room, op.instance, op.kind, op.content); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *Runtime) accessOperation(ctx context.Context, id string) error {
	s := r.Store
	i, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	op, _ := i.State["access_operation"].(map[string]any)
	if op == nil {
		return nil
	}
	if r.Admin == nil {
		return fmt.Errorf("room admin unavailable")
	}
	action := "invite"
	if str(op, "command") == "remove_participant" {
		action = "remove"
	}
	result, err := r.Admin.Do(ctx, roomadmin.Request{Action: action, RoomID: i.RoomID, Actor: i.OwnerID, UserID: str(op, "user_id"), Username: str(op, "username")})
	if err != nil {
		return err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	i, err = s.get(ctx, tx, id, true)
	if err != nil {
		return err
	}
	pending, _ := i.State["access_operation"].(map[string]any)
	if pending == nil || str(pending, "operation_id") != str(op, "operation_id") {
		return nil
	}
	if result.Member != nil {
		acl, _ := i.State["acl"].(map[string]any)
		acl[result.Member.UserID] = "editor"
		shared, _ := i.State["shared"].(map[string]any)
		people := array(shared, "participants")
		found := false
		for _, raw := range people {
			p := raw.(map[string]any)
			if str(p, "user_id") == result.Member.UserID {
				p["state"] = result.Member.State
				found = true
			}
		}
		if !found {
			people = append(people, map[string]any{"user_id": result.Member.UserID, "username": result.Member.Username, "state": result.Member.State})
		}
		shared["participants"] = people
	} else if action == "remove" {
		shared, _ := i.State["shared"].(map[string]any)
		for _, raw := range array(shared, "participants") {
			p := raw.(map[string]any)
			if str(p, "user_id") == str(op, "user_id") {
				p["state"] = "leave"
			}
		}
	}
	delete(i.State, "access_operation")
	i.State["revision"] = revision(i.State) + 1
	b, _ := json.Marshal(i.State)
	if _, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET state=$2 WHERE id=$1", s.table("instances")), id, b); err != nil {
		return err
	}
	content := map[string]any{"plugin": s.Name, "instance_id": id, "state": i.State, "view": View(i.State)}
	if err = s.insertOp(ctx, tx, "access:"+str(op, "operation_id"), hash(content), i.RoomID, id, "m.plugin.state.updated", content); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (r *Runtime) MaintainSharing(ctx context.Context) error {
	rows, err := r.Store.Pool.Query(ctx, fmt.Sprintf("SELECT id FROM %s WHERE state ? 'access_operation' OR state->'transfer'->>'phase' IN ('preparing','frozen') ORDER BY id LIMIT 50", r.Store.table("instances")))
	if err != nil {
		return err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	for _, id := range ids {
		if r.Admin == nil {
			continue
		}
		if err = r.accessOperation(ctx, id); err != nil {
			slog.Warn("access operation pending", "instance", id, "error", err)
			continue
		}
		if err = r.Transfer(ctx, id); err != nil {
			slog.Warn("transfer pending", "instance", id, "error", err)
		}
	}
	rows, err = r.Store.Pool.Query(ctx, fmt.Sprintf("SELECT event FROM %s WHERE NOT done LIMIT 50", r.Store.table("shared_creations")))
	if err != nil {
		return err
	}
	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (protocol.Event, error) {
		var b []byte
		var e protocol.Event
		err := row.Scan(&b)
		if err == nil {
			err = json.Unmarshal(b, &e)
		}
		return e, err
	})
	if err != nil {
		return err
	}
	for _, e := range events {
		if _, err = r.Handle(ctx, e); err != nil {
			slog.Warn("creation pending", "event", e.EventID, "error", err)
		}
	}
	return nil
}
func (r *Runtime) createSharedEvent(ctx context.Context, e protocol.Event) (*protocol.Operation, error) {
	s := r.Store
	var c protocol.Command
	if json.Unmarshal(e.Content, &c) != nil || !transferable(s.Name) || e.Type != "m.plugin.command" || e.EventID == "" || c.Plugin != s.Name {
		return nil, ErrInvalid
	}
	if op, err := s.Operation(ctx, e.EventID); err == nil {
		h, _ := eventHash(e)
		var saved string
		if err = s.Pool.QueryRow(ctx, fmt.Sprintf("SELECT input_hash FROM %s WHERE id=$1", s.table("operations")), e.EventID).Scan(&saved); err != nil || saved != h {
			return nil, ErrConflict
		}
		return op, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	currentHash, err := eventHash(e)
	if err != nil {
		return nil, err
	}
	var recoveredID string
	recoveredErr := s.Pool.QueryRow(ctx, fmt.Sprintf("SELECT i.id FROM %s i JOIN %s c ON c.id=$1 AND c.input_hash=$2 WHERE i.request->>'idempotency_key'=$1 LIMIT 1", s.table("instances"), s.table("shared_creations")), e.EventID, currentHash).Scan(&recoveredID)
	if recoveredErr == nil {
		inst, err := s.Get(ctx, recoveredID)
		if err != nil {
			return nil, err
		}
		return r.finishCreation(ctx, e, c, inst, currentHash)
	}
	if !errors.Is(recoveredErr, pgx.ErrNoRows) {
		return nil, recoveredErr
	}
	actor := e.SenderID
	if c.Delegation != nil {
		var denied, retry error
		actor, denied, retry = r.delegationActor(ctx, e, c)
		if retry != nil {
			return nil, retry
		}
		if denied != nil {
			return nil, denied
		}
	}
	title := str(c.Payload, "title")
	data, ok := c.Payload["data"].(map[string]any)
	if !ok {
		return nil, ErrInvalid
	}
	for k := range data {
		if strings.HasPrefix(k, "_") {
			return nil, ErrInvalid
		}
	}
	req := protocol.Create{Key: e.EventID, RoomID: e.RoomID, OwnerID: actor, Title: title, Data: data}
	if _, err := Initial(s.Name, req); err != nil {
		return nil, fmt.Errorf("%w: invalid creation", ErrInvalid)
	}
	h, err := eventHash(e)
	if err != nil {
		return nil, err
	}
	b, _ := json.Marshal(e)
	_, err = s.Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s(id,input_hash,event) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", s.table("shared_creations")), e.EventID, h, b)
	if err != nil {
		return nil, err
	}
	var prior string
	if err = s.Pool.QueryRow(ctx, fmt.Sprintf("SELECT input_hash FROM %s WHERE id=$1", s.table("shared_creations")), e.EventID).Scan(&prior); err != nil || prior != h {
		return nil, ErrConflict
	}
	if c.Command == "create_personal" {
		inst, err := s.Create(ctx, req)
		if err != nil {
			return nil, err
		}
		return r.finishCreation(ctx, e, c, inst, h)
	}
	key := sharingUUID(s.Name + ":" + e.EventID)
	prepared, err := r.prepareShared(ctx, e.RoomID, actor, key, array(c.Payload, "invitees"))
	if err != nil {
		return nil, err
	}
	req.RoomID = prepared.RoomID
	req.Data = clone(data)
	req.Data["_shared"] = sharedMetadata(key, prepared)
	req.Data["_acl"] = sharedACL(actor, prepared)
	inst, err := s.Create(ctx, req)
	if err != nil {
		return nil, err
	}
	return r.finishCreation(ctx, e, c, inst, h)
}

func (s *Store) abortTransfer(ctx context.Context, id string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	i, err := s.get(ctx, tx, id, true)
	if err != nil {
		return err
	}
	transfer, _ := i.State["transfer"].(map[string]any)
	if transfer == nil || (str(transfer, "phase") != "frozen" && str(transfer, "phase") != "preparing") {
		return nil
	}
	operation := str(transfer, "operation_id")
	delete(i.State, "transfer")
	i.State["sharing_error"] = "Could not share. Check the participants and try again."
	i.State["revision"] = revision(i.State) + 1
	b, _ := json.Marshal(i.State)
	if _, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET state=$2 WHERE id=$1", s.table("instances")), id, b); err != nil {
		return err
	}
	content := map[string]any{"plugin": s.Name, "instance_id": id, "state": i.State, "view": View(i.State)}
	if err = s.insertOp(ctx, tx, "sharing-aborted:"+operation, hash(content), i.RoomID, id, "m.plugin.state.updated", content); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Runtime) finishCreation(ctx context.Context, e protocol.Event, c protocol.Command, inst *protocol.Instance, h string) (*protocol.Operation, error) {
	s := r.Store
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	content := referenceMessage(blockReference(s.Name, inst.ID, inst.RoomID, str(inst.View, "title")))
	if c.Command == "create_personal" {
		content = map[string]any{"schema": "brian.message", "version": "1.0", "body": []any{map[string]any{"type": "plugin", "plugin": s.Name, "version": "1.0", "schema_version": "1.0", "instance_id": inst.ID, "view": inst.View, "fallback_markdown": inst.Fallback}}}
	}
	if err = s.insertOp(ctx, tx, e.EventID, h, e.RoomID, inst.ID, "m.ui.message", content); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET done=true WHERE id=$1", s.table("shared_creations")), e.EventID); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.Operation(ctx, e.EventID)
}
