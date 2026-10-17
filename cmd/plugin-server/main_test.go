package main

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/thiagoribeiro/agent-blocks/internal/roomadmin"
)

func TestMembershipFailurePreservesItsStatus(t *testing.T) {
	for _, status := range []int{400, 403, 404, 503} {
		w := httptest.NewRecorder()
		failure(w, fmt.Errorf("access check: %w", &roomadmin.Error{Status: status}))
		if w.Code != status {
			t.Fatalf("expected %d, got %d", status, w.Code)
		}
	}
}
