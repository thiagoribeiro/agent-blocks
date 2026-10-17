package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"github.com/thiagoribeiro/agent-blocks/internal/roomadmin"
	"net/url"
	"os"
	"strings"
	"time"
)

var delegatedCommands = map[string]map[string]bool{
	"monthly_bills": {"create_shared": true, "update_bill": true, "add_bill": true, "archive_bill": true, "open_period": true, "confirm_amount": true, "refresh": true, "rename": true},
	"checklist":     {"create_shared": true, "rename": true, "check_item": true, "uncheck_item": true, "add_item": true, "remove_item": true, "reorder_item": true, "complete": true, "archive": true, "cancel": true, "refresh": true},
	"poll":          {"rename": true, "vote": true, "change_vote": true, "close": true, "cancel": true, "refresh": true},
	"form_stateful": {"rename": true, "save": true, "submit": true, "cancel": true, "refresh": true},
	"workflow":      {"refresh": true},
}

// Network verification happens before the existing per-instance transaction.
// Temporary failures must not become permanent rejection journal entries.
func (r *Runtime) delegationActor(ctx context.Context, e protocol.Event, c protocol.Command) (string, error, error) {
	d := c.Delegation
	if !r.DelegatingAgents[e.SenderID] || !delegatedCommands[r.Store.Name][c.Command] || c.Plugin != r.Store.Name ||
		d.RequestEventID == "" || d.RequestRoomSeq <= 0 || d.RequestRoomSeq >= e.RoomSeq ||
		c.ExpectedRevision == nil || *c.ExpectedRevision <= 0 {
		return "", ErrForbidden, nil
	}
	if r.NimletToken == "" {
		return "", nil, fmt.Errorf("delegation origin verification unavailable")
	}
	var history struct {
		Chunk []protocol.Event `json:"chunk"`
	}
	code, err := r.call(ctx, "GET", r.NimletURL+"/rooms/"+url.PathEscape(e.RoomID)+"/messages?dir=f&from="+fmt.Sprint(d.RequestRoomSeq-1)+"&limit=1", r.NimletToken, nil, &history)
	if err != nil {
		if code == 403 || code == 404 {
			return "", ErrForbidden, nil
		}
		return "", nil, fmt.Errorf("delegation origin verification unavailable: %w", err)
	}
	if len(history.Chunk) != 1 {
		return "", ErrForbidden, nil
	}
	source := history.Chunk[0]
	var content struct {
		Body    string `json:"body"`
		MsgType string `json:"msgtype"`
	}
	if source.EventID != d.RequestEventID || source.RoomSeq != d.RequestRoomSeq || source.RoomID != e.RoomID ||
		source.SenderID == "" || source.SenderID == e.SenderID || (source.Type != "m.room.message" && source.Type != "m.text") ||
		json.Unmarshal(source.Content, &content) != nil || strings.TrimSpace(content.Body) == "" || (content.MsgType != "" && content.MsgType != "m.text") {
		return "", ErrForbidden, nil
	}
	return source.SenderID, nil, nil
}

func (r *Runtime) Handle(ctx context.Context, e protocol.Event) (*protocol.Operation, error) {
	var c protocol.Command
	if err := json.Unmarshal(e.Content, &c); err != nil {
		return nil, err
	}
	if c.Command == "create_shared" || c.Command == "create_personal" {
		if c.Command == "create_shared" && !r.SharingEnabled {
			var existing bool
			if err := r.Store.Pool.QueryRow(ctx, fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE id=$1)", r.Store.table("shared_creations")), e.EventID).Scan(&existing); err != nil {
				return nil, err
			}
			if !existing {
				return r.Store.RejectEvent(ctx, e, "Sharing unavailable")
			}
		}
		op, err := r.createSharedEvent(ctx, e)
		var administrative *roomadmin.Error
		permanent := errors.As(err, &administrative) && (administrative.Status == 400 || administrative.Status == 403)
		if errors.Is(err, ErrInvalid) || errors.Is(err, ErrForbidden) || permanent {
			return r.Store.RejectEvent(ctx, e, "Creation not authorized or invalid data")
		}
		return op, err
	}
	// A committed replay is independent of membership services, but never re-executes.
	if _, err := r.Store.Operation(ctx, e.EventID); err == nil {
		return r.Store.handle(ctx, e, "", nil)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if c.Command == "rename" && os.Getenv("BRIAN_SURFACE_NAMES_ENABLED") == "false" {
		return r.Store.handle(ctx, e, "", ErrForbidden)
	}
	if !r.SharingEnabled && (c.Command == "preview_share" || c.Command == "share" || c.Command == "invite_participant" || c.Command == "remove_participant" || c.Command == "cancel_transfer") {
		return r.Store.handle(ctx, e, "", ErrForbidden)
	}
	instance, err := r.Store.Get(ctx, c.InstanceID)
	if err == nil && instance.RoomID == e.RoomID && instance.State["shared"] != nil {
		if r.Admin == nil {
			return nil, fmt.Errorf("membership verification unavailable")
		}
		actor := e.SenderID
		if c.Delegation != nil {
			var denied, retry error
			actor, denied, retry = r.delegationActor(ctx, e, c)
			if retry != nil {
				return nil, retry
			}
			if denied != nil {
				return r.Store.handle(ctx, e, actor, denied)
			}
		}
		result, err := r.Admin.Do(ctx, roomadmin.Request{Action: "members", RoomID: e.RoomID, Actor: actor})
		if err != nil {
			return nil, err
		}
		if err = r.Store.SyncMembers(ctx, instance.ID, result.Members); err != nil {
			return nil, err
		}
	}
	if c.Delegation == nil && c.Command == "preview_share" && err == nil && instance.RoomID == e.RoomID && instance.OwnerID == e.SenderID {
		if r.Admin == nil {
			return nil, fmt.Errorf("room admin unavailable")
		}
		for _, raw := range array(c.Payload, "invitees") {
			_, ok := raw.(string)
			if !ok {
				return r.Store.handle(ctx, e, "", ErrInvalid)
			}
		}
		members, validateErr := r.snapshotParticipants(ctx, e.RoomID, e.SenderID, sharingUUID(r.Store.Name+":"+e.EventID), array(c.Payload, "invitees"))
		if validateErr != nil {
			var permanent *roomadmin.Error
			if errors.As(validateErr, &permanent) && (permanent.Status == 400 || permanent.Status == 403) {
				return r.Store.handle(ctx, e, "", ErrInvalid)
			}
			return nil, validateErr
		}
		other := false
		for _, m := range members {
			if m.UserID != e.SenderID {
				other = true
			}
		}
		if !other {
			return r.Store.handle(ctx, e, "", fmt.Errorf("Choose who to share with. No invitation was sent."))
		}
		data, _ := json.Marshal(members)
		var participants []any
		_ = json.Unmarshal(data, &participants)
		preview := map[string]any{"token": sharingUUID(r.Store.Name + ":" + e.EventID), "participants": participants, "invitees": array(c.Payload, "invitees"), "expires_at": time.Now().Add(10 * time.Minute).UTC().Format(time.RFC3339Nano)}
		return r.Store.handlePrepared(ctx, e, "", nil, preview)
	}
	if c.Delegation == nil {
		return r.Store.Handle(ctx, e)
	}
	actor, denied, retry := r.delegationActor(ctx, e, c)
	if retry != nil {
		return nil, retry
	}
	return r.Store.handle(ctx, e, actor, denied)
}
