package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"github.com/thiagoribeiro/agent-blocks/internal/roomadmin"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Runtime struct {
	SurfacePublishers                                                           map[string]bool
	Admin                                                                       *roomadmin.Client
	SharingEnabled                                                              bool
	Store                                                                       *Store
	Wake                                                                        chan struct{}
	Client                                                                      *http.Client
	Token, PublicURL, RouterURL, TramaURL, NimletURL, NimletToken, RiteSmithURL string
	DelegatingAgents                                                            map[string]bool
}

func (r *Runtime) call(ctx context.Context, method, target, token string, body any, out any) (int, error) {
	var b []byte
	var err error
	if body != nil {
		b, err = json.Marshal(body)
		if err != nil {
			return 0, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := r.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return res.StatusCode, protocol.RemoteError(res.StatusCode, body)
	}
	if out != nil && res.StatusCode != 204 {
		err = json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(out)
	}
	return res.StatusCode, err
}
func (r *Runtime) ResultDefinition(id string) map[string]any {
	token := map[string]string{"Authorization": "Bearer " + protocol.OperationToken(r.Token, id), "Content-Type": "application/json"}
	task := func(name, target, next string) map[string]any {
		n := map[string]any{"kind": "task", "id": name, "action": map[string]any{"mode": "sync", "request": map[string]any{"url": target, "verb": "POST", "headers": token, "body": map[string]any{}}}}
		if next != "" {
			n["next"] = next
		}
		return n
	}
	return map[string]any{"name": "brian-" + r.Store.Name + "-result", "version": "1.0", "failureHandling": map[string]any{"type": "backoff", "maxAttempts": 8, "initialDelayMillis": 1000, "maxDelayMillis": 30000}, "entrypoint": "publish", "nodes": []any{
		task("publish", strings.TrimRight(r.RouterURL, "/")+"/plugins/"+r.Store.Name+"/operations/"+url.PathEscape(id)+"/publish", "complete"),
		task("complete", strings.TrimRight(r.PublicURL, "/")+"/operations/"+url.PathEscape(id)+"/complete", "")}}
}
func (r *Runtime) Recover(ctx context.Context) error {
	if err := r.MaintainSharing(ctx); err != nil {
		slog.Warn("sharing maintenance pending", "plugin", r.Store.Name, "error", err)
	}
	ids, err := r.Store.Pending(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		op, err := r.Store.Operation(ctx, id)
		if err != nil {
			return err
		}
		active := false
		for _, execution := range op.Executions {
			var wf map[string]any
			code, e := r.call(ctx, "GET", r.TramaURL+"/workflows/"+url.PathEscape(execution), "", nil, &wf)
			if e != nil {
				return e
			}
			if code == 204 {
				continue
			}
			switch str(wf, "status") {
			case "SUCCEEDED":
				if err = r.Store.Complete(ctx, id); err != nil {
					return err
				}
				active = true
			case "FAILED":
			default:
				active = true
			}
		}
		if active {
			continue
		}
		var resp struct {
			ID string `json:"id"`
		}
		_, err = r.call(ctx, "POST", r.TramaURL+"/workflows/run", "", map[string]any{"definition": r.ResultDefinition(id), "payload": map[string]any{}}, &resp)
		if err != nil {
			return err
		}
		if resp.ID == "" {
			return fmt.Errorf("missing Trama execution id")
		}
		if err = r.Store.Track(ctx, id, resp.ID); err != nil {
			return err
		}
	}
	return nil
}

// expiryEvent is the synthetic command that resolves an expired approval.
func expiryEvent(i *protocol.Instance) protocol.Event {
	b, _ := json.Marshal(protocol.Command{Plugin: "approval", InstanceID: i.ID, Command: "approve", Payload: map[string]any{}})
	return protocol.Event{EventID: "expiry:" + i.ID, RoomID: i.RoomID, SenderID: i.OwnerID, Type: "m.plugin.command", Content: b}
}

func (r *Runtime) Sync(ctx context.Context) error {
	if r.NimletToken == "" {
		return nil
	}
	since, err := r.Store.Cursor(ctx, "sync")
	if err != nil {
		return err
	}
	var batch struct {
		Next   string           `json:"next_batch"`
		Events []protocol.Event `json:"events"`
	}
	if _, numeric := strconv.ParseInt(since, 10, 64); numeric == nil && since != "" {
		return fmt.Errorf("sync cursor %q is a legacy numeric position: convert it with `nimlet user sync-cursor --at %s`", since, since)
	}
	target := r.NimletURL + "/sync?timeout=1000"
	if since != "" {
		target += "&since=" + url.QueryEscape(since)
	}
	code, err := r.call(ctx, "GET", target, r.NimletToken, nil, &batch)
	if code == http.StatusBadRequest && since != "" {
		// Never restart silently: replaying from the beginning would run commands from before
		// PLUGIN_START_CURSOR. A cursor of another account means the token was rotated.
		return fmt.Errorf("nimlet rejected the sync cursor (issued to another account?): %w", err)
	}
	if err != nil {
		return err
	}
	if err = r.handleCommands(ctx, batch.Events); err != nil {
		return err
	}
	return r.Store.SaveCursor(ctx, "sync", batch.Next)
}

// handleCommands handles this plugin's commands among events, in order, and
// ignores everything else.
func (r *Runtime) handleCommands(ctx context.Context, events []protocol.Event) error {
	for _, e := range events {
		if e.Type != "m.plugin.command" {
			continue
		}
		var c protocol.Command
		if json.Unmarshal(e.Content, &c) != nil || c.Plugin != r.Store.Name {
			continue
		}
		if _, err := r.Handle(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// Reconcile advances a durable room cursor independently of live delivery.
// Only an explicit operator rewind replays already confirmed history.
func (r *Runtime) Reconcile(ctx context.Context) error {
	instances, err := r.Store.Instances(ctx)
	if err != nil {
		return err
	}
	anchors := map[string]int64{}
	for _, i := range instances {
		pos, ok := anchors[i.RoomID]
		if !ok || i.SourcePos < pos {
			anchors[i.RoomID] = i.SourcePos
		}
	}
	if r.NimletToken != "" {
		for room, anchor := range anchors {
			cursorKey := "replay:" + room
			saved, err := r.Store.Cursor(ctx, cursorKey)
			if err != nil {
				return err
			}
			from, _ := strconv.ParseInt(saved, 10, 64) // a room_seq; "" (none yet) is 0
			if from < anchor {
				from = anchor
			}
			// Bounded work per cycle; never reset a completed sweep to its anchor.
			for page := 0; page < 5; page++ {
				var history struct {
					End   int64            `json:"end"`
					Chunk []protocol.Event `json:"chunk"`
				}
				_, err = r.call(ctx, "GET", r.NimletURL+"/rooms/"+url.PathEscape(room)+"/messages?dir=f&from="+fmt.Sprint(from)+"&limit=100", r.NimletToken, nil, &history)
				if err != nil {
					return err
				}
				if err = r.handleCommands(ctx, history.Chunk); err != nil {
					return err
				}
				if len(history.Chunk) == 0 {
					break
				}
				if history.End <= from {
					return fmt.Errorf("history cursor did not advance")
				}
				from = history.End
				if err = r.Store.SaveCursor(ctx, cursorKey, strconv.FormatInt(from, 10)); err != nil {
					return err
				}
				if len(history.Chunk) < 100 {
					break
				}
			}
		}
	}
	for _, i := range instances {
		if r.Store.Name == "approval" && str(i.State, "status") == "pending" {
			expires, e := time.Parse(time.RFC3339, str(i.State, "expires_at"))
			if e == nil && !expires.After(time.Now()) {
				_, err = r.Store.Handle(ctx, expiryEvent(i))
				if err != nil {
					return err
				}
			}
		}
		if r.Store.Name == "workflow" {
			if err = r.refreshWorkflow(ctx, i); err != nil {
				slog.Warn("provider refresh failed", "instance", i.ID, "error", err)
			}
		}
	}
	return nil
}
func (r *Runtime) refreshWorkflow(ctx context.Context, i *protocol.Instance) error {
	base, path := r.TramaURL, "/workflows/"
	if str(i.State, "provider") == "ritesmith" {
		base, path = r.RiteSmithURL, "/plans/"
	}
	if base == "" {
		return fmt.Errorf("provider not configured")
	}
	var provider map[string]any
	code, err := r.call(ctx, "GET", base+path+url.PathEscape(str(i.State, "execution_id")), "", nil, &provider)
	if err != nil {
		return err
	}
	if code == 204 {
		return fmt.Errorf("execution not found")
	}
	mapping := map[string]string{"IN_PROGRESS": "running", "SLEEPING": "waiting", "WAITING_CALLBACK": "waiting", "WAITING_JOIN": "waiting", "SUCCEEDED": "completed", "FAILED": "failed", "executing": "running", "proposed": "waiting", "approved": "created", "completed": "completed", "failed": "failed", "cancelled": "cancelled", "rejected": "cancelled", "blocked": "failed", "superseded": "cancelled"}
	status, ok := mapping[str(provider, "status")]
	if !ok {
		return nil
	}
	state := clone(i.State)
	state["status"] = status
	state["provider_status"] = str(provider, "status")
	state["summary"] = str(provider, "summary")
	state["failure_description"] = str(provider, "failureDescription")
	if str(i.State, "provider") == "trama" {
		var steps []map[string]any
		_, err = r.call(ctx, "GET", base+path+url.PathEscape(str(i.State, "execution_id"))+"/steps", "", nil, &steps)
		if err != nil {
			return err
		}
		items := []any{}
		for n, step := range steps {
			st := "failed"
			if step["success"] == true {
				st = "completed"
			}
			items = append(items, map[string]any{"id": fmt.Sprint(n), "title": str(step, "stepName"), "status": st})
		}
		state["steps"] = items
	}
	state["revision"] = i.State["revision"]
	if hash(state) == hash(i.State) {
		return nil
	}
	tx, err := r.Store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	current, err := r.Store.get(ctx, tx, i.ID, true)
	if err != nil {
		return err
	}
	if revision(current.State) != revision(i.State) {
		return nil
	}
	state["revision"] = revision(i.State) + 1
	b, _ := json.Marshal(state)
	_, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET state=$2 WHERE id=$1", r.Store.table("instances")), i.ID, b)
	if err != nil {
		return err
	}
	content := map[string]any{"plugin": r.Store.Name, "instance_id": i.ID, "state": state, "view": View(state)}
	id := "provider:" + i.ID + ":" + fmt.Sprint(revision(state))
	if err = r.Store.insertOp(ctx, tx, id, hash(state), i.RoomID, i.ID, "m.plugin.state.updated", content); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (r *Runtime) Notify() {
	select {
	case r.Wake <- struct{}{}:
	default:
	}
}
func (r *Runtime) Run(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	maintenance := true
	for {
		cycle, cancel := context.WithTimeout(ctx, 90*time.Second)
		if maintenance {
			if err := r.Sync(cycle); err != nil {
				slog.Warn("plugin sync failed", "plugin", r.Store.Name, "error", err)
			}
			if err := r.Reconcile(cycle); err != nil {
				slog.Warn("plugin reconciliation failed", "plugin", r.Store.Name, "error", err)
			}
		}
		if err := r.Recover(cycle); err != nil {
			slog.Warn("plugin admission failed", "plugin", r.Store.Name, "error", err)
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			maintenance = true
		case <-r.Wake:
			maintenance = false
		}
	}
}
