package protocol

import (
	"encoding/json"
	"fmt"
)

// HTTPError preserves the upstream status without exposing its response body.
type HTTPError struct {
	Status    int    `json:"upstream_status"`
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
}

func (e *HTTPError) Error() string { return fmt.Sprintf("remote returned %d", e.Status) }

func RemoteError(status int, body []byte) *HTTPError {
	e := &HTTPError{Status: status, Code: "publication_failed", Retryable: status >= 500 || status == 408 || status == 429}
	var details struct {
		Retryable *bool `json:"retryable"`
	}
	if json.Unmarshal(body, &details) == nil && details.Retryable != nil {
		e.Retryable = *details.Retryable
	}
	return e
}
