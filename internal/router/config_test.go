package router

import (
	"os"
	"path/filepath"
	"testing"
)

func writePlugins(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "plugins.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func setBaseEnv(t *testing.T, plugins string) {
	t.Setenv("PORT", "4000")
	t.Setenv("HS_TOKEN", "hs")
	t.Setenv("AS_TOKEN", "as")
	t.Setenv("AGENT_BLOCKS_INTERNAL_TOKEN", "internal")
	t.Setenv("PLUGINS_FILE", writePlugins(t, plugins))
}

func TestLoadConfigAcceptsValidRegistry(t *testing.T) {
	setBaseEnv(t, `[{"name":"approval","url":"http://approval:4010","token":"a"},{"name":"monthly_bills","url":"https://bills","token":"b"}]`)
	t.Setenv("NIMLET_URL", "http://nimlet:8080")
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NimletURL != "http://nimlet:8080" || len(cfg.Plugins) != 2 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestLoadConfigRejectsInvalidSettings(t *testing.T) {
	for name, tc := range map[string]struct {
		env     map[string]string
		plugins string
	}{
		"missing hs token": {env: map[string]string{"HS_TOKEN": ""}, plugins: `[]`},
		"invalid port":     {env: map[string]string{"PORT": "70000"}, plugins: `[]`},
		"missing file":     {env: map[string]string{"PLUGINS_FILE": ""}, plugins: `[]`},
		"unknown plugin":   {plugins: `[{"name":"pizza","url":"http://p","token":"a"}]`},
		"duplicate plugin": {plugins: `[{"name":"poll","url":"http://a","token":"a"},{"name":"poll","url":"http://b","token":"b"}]`},
		"shared token":     {plugins: `[{"name":"poll","url":"http://a","token":"same"},{"name":"checklist","url":"http://b","token":"same"}]`},
		"empty token":      {plugins: `[{"name":"poll","url":"http://a","token":""}]`},
		"non-http url":     {plugins: `[{"name":"poll","url":"file:///etc/passwd","token":"a"}]`},
		"malformed json":   {plugins: `{`},
	} {
		t.Run(name, func(t *testing.T) {
			setBaseEnv(t, tc.plugins)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if _, err := LoadConfig(); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
