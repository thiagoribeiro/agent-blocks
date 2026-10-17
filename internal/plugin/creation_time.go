package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
	"net/url"
	"time"
)

// Read only the authorized source stream; export the timestamp, never private content.
func (r *Runtime) originalCreationTime(ctx context.Context, i *protocol.Instance) (string, error) {
	if r.NimletToken == "" {
		return str(i.State, "created_at"), nil
	} // isolated store-level fixtures
	from := i.SourcePos
	for {
		var page struct {
			Chunk []protocol.Event `json:"chunk"`
			End   int64            `json:"end"`
		}
		_, err := r.call(ctx, "GET", r.NimletURL+"/rooms/"+url.PathEscape(i.RoomID)+fmt.Sprintf("/messages?dir=f&from=%d&limit=100", from), r.NimletToken, nil, &page)
		if err != nil {
			return "", err
		}
		for _, e := range page.Chunk {
			if e.RoomID != i.RoomID || e.Type != "m.ui.message" || e.OriginServerTS <= 0 || (!r.SurfacePublishers[e.SenderID] && !r.DelegatingAgents[e.SenderID]) {
				continue
			}
			var content struct {
				Schema  string `json:"schema"`
				Version string `json:"version"`
				Body    []struct {
					Type, Plugin string
					ID           string `json:"instance_id"`
				} `json:"body"`
			}
			if json.Unmarshal(e.Content, &content) != nil || content.Schema != "brian.message" || content.Version != "1.0" {
				continue
			}
			for _, b := range content.Body {
				if b.Type == "plugin" && b.Plugin == r.Store.Name && b.ID == i.ID {
					return time.UnixMilli(e.OriginServerTS).UTC().Format(time.RFC3339Nano), nil
				}
			}
		}
		if len(page.Chunk) == 0 || page.End <= from {
			return "", nil
		}
		from = page.End
	}
}
