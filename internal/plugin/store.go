package plugin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thiagoribeiro/agent-blocks/internal/catalog"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
)

type Store struct {
	Pool         *pgxpool.Pool
	Name, prefix string
}

func Open(ctx context.Context, dsn, name string) (*Store, error) {
	if !catalog.Supported(name) {
		return nil, fmt.Errorf("unsupported plugin")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	s := &Store{Pool: pool, Name: name, prefix: pgx.Identifier{"brian_" + name}.Sanitize()}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err = s.migrate(ctx); err == nil {
		err = s.migrateSharing(ctx)
	}
	if err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) table(n string) string { return s.prefix + "." + n }
func (s *Store) migrate(ctx context.Context) error {
	q := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;
 CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY, create_key text UNIQUE NOT NULL, room_id text NOT NULL, owner_id text NOT NULL, source_pos bigint NOT NULL, request jsonb NOT NULL, state jsonb NOT NULL);
 CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY, input_hash text NOT NULL, room_id text NOT NULL, instance_id text NOT NULL, event_type text NOT NULL, content jsonb NOT NULL, done boolean NOT NULL DEFAULT false, executions jsonb NOT NULL DEFAULT '[]', created_at timestamptz NOT NULL DEFAULT now());
 CREATE INDEX IF NOT EXISTS operations_pending ON %s (created_at) WHERE NOT done;
 CREATE TABLE IF NOT EXISTS %s (id text PRIMARY KEY, pos text NOT NULL);`, s.prefix, s.table("instances"), s.table("operations"), s.table("operations"), s.table("cursors"))
	if _, err := s.Pool.Exec(ctx, q); err != nil {
		return err
	}
	if s.Name == "agent_status" {
		// Durable retry scheduling only for telemetry. Existing operations and
		// transaction IDs remain intact; the other plugin queues are unchanged.
		q := fmt.Sprintf(`ALTER TABLE %s
 ADD COLUMN IF NOT EXISTS status_publish_failures integer NOT NULL DEFAULT 0,
 ADD COLUMN IF NOT EXISTS status_publish_after timestamptz NOT NULL DEFAULT now(),
 ADD COLUMN IF NOT EXISTS status_publish_suspended text;
 CREATE INDEX IF NOT EXISTS operations_status_pending ON %s(instance_id,created_at,id) WHERE NOT done;`, s.table("operations"), s.table("operations"))
		if _, err := s.Pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	// One-shot upgrade from the legacy schema, whose cursors held numbers. The values are
	// kept: the sync cursor must be converted by the operator (nimlet user sync-cursor), never
	// silently restarted, or commands from before PLUGIN_START_CURSOR would run. Replay cursors
	// are dropped; replays restart from each instance's anchor and the journal dedupes them.
	// source_pos (also a global position) is remapped to room_seq during the upgrade window.
	var legacy bool
	if err := s.Pool.QueryRow(ctx, `SELECT data_type = 'bigint' FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = 'cursors' AND column_name = 'pos'`, "brian_"+s.Name).Scan(&legacy); err != nil {
		return err
	}
	if !legacy {
		return nil
	}
	_, err := s.Pool.Exec(ctx, fmt.Sprintf(`BEGIN;
 ALTER TABLE %s ALTER COLUMN pos TYPE text;
 DELETE FROM %s WHERE id LIKE 'replay:%%';
 COMMIT;`, s.table("cursors"), s.table("cursors")))
	return err
}

// eventHash identifies a command event in the journal: a replay must carry the same input.
// The position is left out: it is server-assigned for the event ID (and changed meaning when
// the global stream_pos became Nimlet's per-room room_seq).
func eventHash(e protocol.Event) (string, error) {
	var canonical any
	if err := json.Unmarshal(e.Content, &canonical); err != nil {
		return "", err
	}
	stable := e
	stable.RoomSeq = 0
	stable.OriginServerTS = 0
	stable.Content, _ = json.Marshal(canonical)
	return hash(stable), nil
}

// Rehash rewrites the journal's input hashes with eventHash, once, after the upgrade from
// the legacy format (whose hashes covered stream_pos). events holds the original command events by ID;
// synthetic expiry commands are rebuilt from their instance. It also aligns each instance's
// stored create request with its remapped source_pos, so repeated creates stay idempotent.
func (s *Store) Rehash(ctx context.Context, events map[string]protocol.Event) (rehashed, skipped int, err error) {
	rows, err := s.Pool.Query(ctx, fmt.Sprintf("SELECT id FROM %s WHERE id NOT LIKE 'create:%%'", s.table("operations")))
	if err != nil {
		return 0, 0, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, 0, err
	}
	for _, id := range ids {
		e, ok := events[id]
		if instance, expiry := strings.CutPrefix(id, "expiry:"); expiry {
			i, gerr := s.Get(ctx, instance)
			if gerr != nil {
				return rehashed, skipped, gerr
			}
			e, ok = expiryEvent(i), true
		}
		if !ok {
			skipped++
			continue
		}
		h, herr := eventHash(e)
		if herr != nil {
			return rehashed, skipped, herr
		}
		if _, err = s.Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET input_hash=$2 WHERE id=$1", s.table("operations")), id, h); err != nil {
			return rehashed, skipped, err
		}
		rehashed++
	}
	_, err = s.Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET request = jsonb_set(request, '{source_pos}', to_jsonb(source_pos))", s.table("instances")))
	return rehashed, skipped, err
}

// JournalIDs lists the journal entries that came from transport events (not synthetic ones).
func (s *Store) JournalIDs(ctx context.Context) ([]string, error) {
	rows, err := s.Pool.Query(ctx, fmt.Sprintf("SELECT id FROM %s WHERE id NOT LIKE 'create:%%' AND id NOT LIKE 'expiry:%%'", s.table("operations")))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func hash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (s *Store) Create(ctx context.Context, r protocol.Create) (*protocol.Instance, error) {
	// Validate new requests before admission. Repeated creates recover an existing instance even after expiry.
	id := hash([]string{s.Name, r.RoomID, r.OwnerID, r.Key})[:32]
	if i, err := s.Get(ctx, id); err == nil {
		if createHash(i.Request) != createHash(r) {
			return nil, ErrConflict
		}
		return i, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	state, err := Initial(s.Name, r)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if transferable(s.Name) {
		state["created_at"] = time.Now().UTC().Format(time.RFC3339Nano)
		if shared, ok := r.Data["_shared"].(map[string]any); ok {
			state["shared"] = shared
			state["acl"] = r.Data["_acl"]
		}
	}
	request, _ := json.Marshal(r)
	b, _ := json.Marshal(state)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id,create_key,room_id,owner_id,source_pos,request,state) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING", s.table("instances")), id, id, r.RoomID, r.OwnerID, r.SourcePos, request, b)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() > 0 {
		content := map[string]any{"plugin": s.Name, "instance_id": id, "state": state, "view": View(state)}
		kind := "m.plugin.instance.created"
		if s.Name == "agent_status" {
			kind = "m.ui.message"
			content = map[string]any{"schema": "brian.message", "version": "1.0", "body": []any{map[string]any{"type": "plugin", "plugin": "agent_status", "version": "1.0", "schema_version": "1.0", "instance_id": id, "view": View(state), "fallback_markdown": Fallback(state), "surface": map[string]any{"default": "pinned", "auto_pin": true, "dismissible": true}}}}
		}
		if err = s.insertOp(ctx, tx, "create:"+id, hash(r), r.RoomID, id, kind, content); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	i, err := s.Get(ctx, id)
	if err == nil && createHash(i.Request) != createHash(r) {
		return nil, ErrConflict
	}
	return i, err
}
func (s *Store) Get(ctx context.Context, id string) (*protocol.Instance, error) {
	return s.get(ctx, s.Pool, id, false)
}

type rowQuery interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func (s *Store) get(ctx context.Context, q rowQuery, id string, lock bool) (*protocol.Instance, error) {
	suffix := ""
	if lock {
		suffix = " FOR UPDATE"
	}
	i := &protocol.Instance{ID: id, Plugin: s.Name}
	var state, req []byte
	err := q.QueryRow(ctx, fmt.Sprintf("SELECT room_id,owner_id,source_pos,request,state FROM %s WHERE id=$1%s", s.table("instances"), suffix), id).Scan(&i.RoomID, &i.OwnerID, &i.SourcePos, &req, &state)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(req, &i.Request); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(state, &i.State); err != nil {
		return nil, err
	}
	i.View = View(i.State)
	i.Fallback = Fallback(i.State)
	return i, nil
}
func (s *Store) insertOp(ctx context.Context, tx pgx.Tx, id, inputHash, room, instance, kind string, content map[string]any) error {
	b, _ := json.Marshal(content)
	_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s(id,input_hash,room_id,instance_id,event_type,content) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING", s.table("operations")), id, inputHash, room, instance, kind, b)
	return err
}
func (s *Store) Handle(ctx context.Context, e protocol.Event) (*protocol.Operation, error) {
	return s.handle(ctx, e, "", nil)
}

// Only Runtime can supply the actor obtained from the authoritative Nimlet
// request. The original event and its agent sender remain intact for audit.
func (s *Store) handle(ctx context.Context, e protocol.Event, delegatedActor string, authorizationErr error) (*protocol.Operation, error) {
	return s.handlePrepared(ctx, e, delegatedActor, authorizationErr, nil)
}
func (s *Store) handlePrepared(ctx context.Context, e protocol.Event, delegatedActor string, authorizationErr error, preview map[string]any) (*protocol.Operation, error) {
	var c protocol.Command
	if err := json.Unmarshal(e.Content, &c); err != nil {
		return nil, err
	}
	if e.Type != "m.plugin.command" || e.EventID == "" || e.RoomID == "" || e.SenderID == "" || c.Plugin != s.Name {
		return nil, fmt.Errorf("invalid command envelope")
	}
	d := json.NewDecoder(bytes.NewReader(e.Content))
	d.DisallowUnknownFields()
	invalid := d.Decode(&protocol.Command{}) != nil || c.InstanceID == "" || c.Command == "" || c.Payload == nil
	inputHash, err := eventHash(e)
	if err != nil {
		return nil, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	// Lock only this plugin instance. Unrelated instances and Nimlet remain concurrent.
	i, lookup := s.get(ctx, tx, c.InstanceID, true)
	var prior string
	err = tx.QueryRow(ctx, fmt.Sprintf("SELECT input_hash FROM %s WHERE id=$1", s.table("operations")), e.EventID).Scan(&prior)
	if err == nil {
		if prior != inputHash {
			return nil, ErrConflict
		}
		tx.Rollback(ctx)
		return s.Operation(ctx, e.EventID)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	var state map[string]any
	var domainErr error
	if invalid {
		domainErr = ErrInvalid
	} else if errors.Is(lookup, pgx.ErrNoRows) {
		domainErr = fmt.Errorf("instance not found")
	} else if lookup != nil {
		return nil, lookup
	} else if i.RoomID != e.RoomID {
		domainErr = ErrForbidden
	} else if authorizationErr != nil {
		domainErr = authorizationErr
	} else if c.Delegation != nil && (delegatedActor == "" || !CanEdit(i.State, i.OwnerID, delegatedActor)) {
		domainErr = ErrForbidden
	} else if (c.Delegation != nil && c.ExpectedRevision == nil) || (c.ExpectedRevision != nil && *c.ExpectedRevision != revision(i.State)) {
		domainErr = fmt.Errorf("revision conflict: read the current instance before retrying")
	} else {
		actor := e.SenderID
		if c.Delegation != nil {
			actor = delegatedActor
		}
		payload := clone(c.Payload)
		delete(payload, "_validated_preview")
		if preview != nil {
			payload["_validated_preview"] = preview
		}
		state, domainErr = Apply(s.Name, i.State, i.OwnerID, actor, c.Command, payload, time.Now())
		if domainErr == nil {
			if c.Command == "share" {
				state["transfer"].(map[string]any)["operation_id"] = e.EventID
				approved := i.State["sharing_preview"].(map[string]any)
				members, _ := json.Marshal(approved["participants"])
				if _, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s(id,participants) VALUES($1,$2) ON CONFLICT DO NOTHING", s.table("sharing_participants")), sharingUUID(s.Name+":"+e.EventID), members); err != nil {
					return nil, err
				}
				delete(state, "sharing_preview")
			}
			if c.Command == "invite_participant" || c.Command == "remove_participant" {
				state["access_operation"].(map[string]any)["operation_id"] = e.EventID
			}
		}
	}
	kind := "m.plugin.state.updated"
	var content map[string]any
	if domainErr != nil {
		kind = "m.plugin.command.rejected"
		content = map[string]any{"plugin": s.Name, "instance_id": c.InstanceID, "command_event_id": e.EventID, "reason": domainErr.Error()}
	} else {
		b, _ := json.Marshal(state)
		_, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET state=$2 WHERE id=$1", s.table("instances")), i.ID, b)
		if err != nil {
			return nil, err
		}
		content = map[string]any{"plugin": s.Name, "instance_id": i.ID, "state": state, "view": View(state), "command_event_id": e.EventID}
	}
	if c.Delegation != nil {
		content["delegation"] = map[string]any{"agent_id": e.SenderID, "requester_id": delegatedActor, "request_event_id": c.Delegation.RequestEventID, "request_room_seq": c.Delegation.RequestRoomSeq}
	}
	if err = s.insertOp(ctx, tx, e.EventID, inputHash, e.RoomID, c.InstanceID, kind, content); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return s.Operation(ctx, e.EventID)
}
func (s *Store) Operation(ctx context.Context, id string) (*protocol.Operation, error) {
	o := &protocol.Operation{ID: id}
	var b, x []byte
	err := s.Pool.QueryRow(ctx, fmt.Sprintf("SELECT room_id,instance_id,event_type,content,done,executions FROM %s WHERE id=$1", s.table("operations")), id).Scan(&o.RoomID, &o.InstanceID, &o.EventType, &b, &o.Done, &x)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(b, &o.Content); err != nil {
		return nil, err
	}
	err = json.Unmarshal(x, &o.Executions)
	return o, err
}
func (s *Store) Pending(ctx context.Context) ([]string, error) {
	rows, err := s.Pool.Query(ctx, fmt.Sprintf("SELECT id FROM %s WHERE NOT done ORDER BY created_at LIMIT 50", s.table("operations")))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

const statusHeadQuery = `SELECT DISTINCT ON (instance_id) id,created_at,status_publish_after,status_publish_suspended
 FROM %s WHERE NOT done ORDER BY instance_id,created_at,id`

// StatusPending returns only the oldest unconfirmed operation per instance.
// A failed creation blocks its own updates, while unrelated instances proceed.
func (s *Store) StatusPending(ctx context.Context) ([]string, error) {
	if s.Name != "agent_status" {
		return nil, fmt.Errorf("status queue required")
	}
	q := fmt.Sprintf(`SELECT id FROM (`+statusHeadQuery+`) first_pending
 WHERE status_publish_after<=clock_timestamp()
 AND status_publish_suspended IS NULL
	 ORDER BY status_publish_after,created_at,id LIMIT 50`, s.table("operations"))
	rows, err := s.Pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (s *Store) RetryStatus(ctx context.Context, id string) (int, error) {
	if s.Name != "agent_status" {
		return 0, fmt.Errorf("status queue required")
	}
	q := fmt.Sprintf(`UPDATE %s SET
 status_publish_failures=status_publish_failures+1,
 status_publish_after=clock_timestamp()+make_interval(secs=>least(300,power(2,least(status_publish_failures,9))*(0.75+random()*0.5)))
 WHERE id=$1 AND NOT done RETURNING status_publish_failures`, s.table("operations"))
	var failures int
	err := s.Pool.QueryRow(ctx, q, id).Scan(&failures)
	return failures, err
}

// Suspension preserves the journal, including unconfirmed creations and updates.
func (s *Store) SuspendStatus(ctx context.Context, room, reason string) error {
	if s.Name != "agent_status" {
		return ErrForbidden
	}
	_, err := s.Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET status_publish_suspended=$2 WHERE room_id=$1 AND NOT done", s.table("operations")), room, reason)
	return err
}

func (s *Store) ResumeStatus(ctx context.Context, room string) error {
	if s.Name != "agent_status" {
		return ErrForbidden
	}
	_, err := s.Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET status_publish_suspended=NULL,status_publish_failures=0,status_publish_after=clock_timestamp() WHERE room_id=$1 AND NOT done", s.table("operations")), room)
	return err
}

// Sleep until the next durable retry. New updates wake RunStatus immediately.
func (s *Store) StatusDelay(ctx context.Context) time.Duration {
	// The parent may have no deadline; pool saturation must not stall this worker.
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var due *time.Time
	err := s.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT min(status_publish_after) FROM (`+statusHeadQuery+`) oldest
 WHERE status_publish_suspended IS NULL`, s.table("operations"))).Scan(&due)
	if err != nil {
		return time.Second
	}
	if due == nil {
		return 30 * time.Second
	}
	delay := time.Until(*due)
	if delay < 10*time.Millisecond {
		return 10 * time.Millisecond
	}
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func (s *Store) Track(ctx context.Context, id, execution string) error {
	_, err := s.Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET executions=(CASE WHEN jsonb_array_length(executions)>=8 THEN executions - 0 ELSE executions END) || to_jsonb($2::text) WHERE id=$1", s.table("operations")), id, execution)
	return err
}
func (s *Store) Complete(ctx context.Context, id string) error {
	_, err := s.Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET done=true WHERE id=$1", s.table("operations")), id)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, fmt.Sprintf(`UPDATE %s t SET phase='completed' WHERE phase='committed' AND NOT EXISTS (SELECT 1 FROM %s o WHERE o.id IN ('create:'||t.target_id,'moved:'||t.operation_id,'redirect:'||t.operation_id) AND NOT o.done)`, s.table("transfers"), s.table("operations")))
	return err
}
func (s *Store) Instances(ctx context.Context) ([]*protocol.Instance, error) {
	rows, err := s.Pool.Query(ctx, fmt.Sprintf("SELECT id FROM %s WHERE state->>'status' NOT IN ('approved','rejected','expired','cancelled','submitted','closed','archived') ORDER BY id", s.table("instances")))
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := []*protocol.Instance{}
	for _, id := range ids {
		i, e := s.Get(ctx, id)
		if e != nil {
			return nil, e
		}
		out = append(out, i)
	}
	return out, nil
}

// Cursors are strings: the sync cursor is Nimlet's opaque next_batch, replay cursors are
// room_seq numbers. A missing cursor is "".
func (s *Store) BootstrapCursor(ctx context.Context, id string, pos string) error {
	_, err := s.Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s(id,pos) VALUES($1,$2) ON CONFLICT DO NOTHING", s.table("cursors")), id, pos)
	return err
}
func (s *Store) Cursor(ctx context.Context, id string) (string, error) {
	var pos string
	err := s.Pool.QueryRow(ctx, fmt.Sprintf("SELECT pos FROM %s WHERE id=$1", s.table("cursors")), id).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return pos, err
}
func (s *Store) SaveCursor(ctx context.Context, id string, pos string) error {
	_, err := s.Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s(id,pos) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET pos=EXCLUDED.pos", s.table("cursors")), id, pos)
	return err
}

// Provisioning can observe an invite becoming joined between retries. Those transport
// snapshots are not part of the caller's immutable creation intent.
func createHash(r protocol.Create) string {
	data := clone(r.Data)
	delete(data, "_shared")
	delete(data, "_acl")
	r.Data = data
	return hash(r)
}
