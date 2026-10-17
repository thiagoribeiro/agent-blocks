// Package nimlet is the minimal Nimlet AppService client used by the router.
package nimlet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/thiagoribeiro/agent-blocks/internal/protocol"
)

// Client sends events to Nimlet on behalf of an AppService.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

// New returns a client that sends events as an AppService with asToken.
func New(baseURL, asToken string) *Client {
	return &Client{
		baseURL: baseURL,
		token:   asToken,
		http:    &http.Client{Timeout: 15 * time.Second},
	}
}

// SendEventResponse is Nimlet's reply to a sent event.
type SendEventResponse struct {
	EventID string `json:"event_id"`
	RoomSeq int64  `json:"room_seq"`
}

func (c *Client) SendEvent(ctx context.Context, appServiceID, roomID, eventType, txnID string, content json.RawMessage) (*SendEventResponse, error) {
	if len(content) == 0 {
		return nil, fmt.Errorf("missing publication content")
	}

	url := fmt.Sprintf("%s/appservice/%s/rooms/%s/send/%s/%s", strings.TrimRight(c.baseURL, "/"), url.PathEscape(appServiceID), url.PathEscape(roomID), url.PathEscape(eventType), url.PathEscape(txnID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(content))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return nil, protocol.RemoteError(res.StatusCode, b)
	}

	var result SendEventResponse
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}
