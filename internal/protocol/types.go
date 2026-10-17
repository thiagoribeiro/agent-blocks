package protocol

import "encoding/json"

type Event struct {
	OriginServerTS int64           `json:"origin_server_ts,omitempty"`
	EventID        string          `json:"event_id"`
	RoomSeq        int64           `json:"room_seq"` // order within the room; not comparable across rooms
	RoomID         string          `json:"room_id"`
	Type           string          `json:"type"`
	SenderID       string          `json:"sender_id"`
	Content        json.RawMessage `json:"content"`
}

type Command struct {
	Plugin           string         `json:"plugin"`
	InstanceID       string         `json:"instance_id"`
	Command          string         `json:"command"`
	Payload          map[string]any `json:"payload"`
	Delegation       *Delegation    `json:"delegation,omitempty"`
	ExpectedRevision *int64         `json:"expected_revision,omitempty"`
}

type Delegation struct {
	RequestEventID string `json:"request_event_id"`
	RequestRoomSeq int64  `json:"request_room_seq"`
}

type Create struct {
	Key         string         `json:"idempotency_key"`
	RoomID      string         `json:"room_id"`
	OwnerID     string         `json:"owner_id"`
	Title       string         `json:"title"`
	Description string         `json:"description"`
	ExpiresAt   string         `json:"expires_at,omitempty"`
	SourcePos   int64          `json:"source_pos"` // room_seq of the message that asked for the instance
	Data        map[string]any `json:"data"`
}

type Instance struct {
	ID        string         `json:"instance_id"`
	Plugin    string         `json:"plugin"`
	RoomID    string         `json:"room_id"`
	OwnerID   string         `json:"owner_id"`
	State     map[string]any `json:"state"`
	View      map[string]any `json:"view"`
	Fallback  string         `json:"fallback_markdown"`
	SourcePos int64          `json:"source_pos"`
	Request   Create         `json:"-"`
}

type Operation struct {
	ID         string         `json:"operation_id"`
	RoomID     string         `json:"room_id"`
	InstanceID string         `json:"instance_id"`
	EventType  string         `json:"event_type"`
	Content    map[string]any `json:"content"`
	Done       bool           `json:"done"`
	Executions []string       `json:"executions"`
}
