package roomadmin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestNoArbitraryCLIOrUnauthenticatedAdministration(t *testing.T) {
	if _, err := Executable("/bin/echo", "default")(context.Background(), "namespace", "disable", "default"); err == nil {
		t.Fatal("arbitrary command")
	}
	s := &Server{Token: "secret", Namespace: "default"}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("POST", "/operation", strings.NewReader(`{}`)))
	if w.Code != http.StatusUnauthorized {
		t.Fatal(w.Code)
	}
}
func TestMembershipCannotBeAssertedByCaller(t *testing.T) {
	s := &Server{Namespace: "default", CLI: func(_ context.Context, args ...string) ([]byte, error) {
		if args[0] == "user" {
			return []byte(`[{"id":"member","username":"member","type":"human"}]`), nil
		}
		return []byte(`[{"user_id":"member","username":"member","state":"invite"}]`), nil
	}}
	if _, err := s.Operation(context.Background(), Request{Action: "remove", RoomID: "11111111-1111-4111-8111-111111111111", Actor: "member", UserID: "other"}); err == nil {
		t.Fatal("invited actor authorized")
	}
}

func TestCLIExcludesAdapterAndPluginCredentials(t *testing.T) {
	t.Setenv("NIMLET_CONSUMER_TOKEN", "must-not-inherit")
	t.Setenv("PLUGIN_TOKEN", "must-not-inherit")
	t.Setenv("NIMLET_ADMIN_DB_DSN", "operator-test-only")
	path := t.TempDir() + "/cli"
	if err := os.WriteFile(path, []byte("#!/bin/sh\n[ -z \"$NIMLET_CONSUMER_TOKEN\" ] && [ -z \"$PLUGIN_TOKEN\" ] && [ \"$NIMLET_ADMIN_DB_DSN\" = operator-test-only ] || exit 1\nprintf '{}'"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := Executable(path, "default")(context.Background(), "user", "show", "Alice"); err != nil {
		t.Fatal(err)
	}
}

func TestCLIProcessesAreBoundedAcrossConcurrentRequests(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/cli"
	script := "#!/bin/sh\nmkdir '" + dir + "/active' || exit 1\nsleep 0.1\nrmdir '" + dir + "/active'\nprintf '{}'\n"
	if err := os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	cli := Executable(path, "default")
	var wg sync.WaitGroup
	for n := 0; n < 3; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cli(context.Background(), "user", "show", "Alice"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}
