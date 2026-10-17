package router

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"

	"github.com/thiagoribeiro/agent-blocks/internal/catalog"
	"github.com/thiagoribeiro/agent-blocks/internal/httpx"
)

type PluginConfig struct {
	Name  string `json:"name"`
	URL   string `json:"url"`
	Token string `json:"token"`
}
type Config struct {
	Port                                       int
	NimletURL, HsToken, AsToken, InternalToken string
	Plugins                                    []PluginConfig
}

// LoadConfig reads the router configuration from the environment and PLUGINS_FILE.
func LoadConfig() (Config, error) {
	port, err := strconv.Atoi(httpx.Env("PORT", "4000"))
	if err != nil || port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("invalid PORT")
	}
	cfg := Config{Port: port, NimletURL: httpx.Env("NIMLET_URL", "http://localhost:8080"), HsToken: os.Getenv("HS_TOKEN"), AsToken: os.Getenv("AS_TOKEN"), InternalToken: os.Getenv("AGENT_BLOCKS_INTERNAL_TOKEN")}
	if cfg.HsToken == "" || cfg.AsToken == "" || cfg.InternalToken == "" {
		return cfg, fmt.Errorf("HS_TOKEN, AS_TOKEN and AGENT_BLOCKS_INTERNAL_TOKEN are required")
	}
	path := os.Getenv("PLUGINS_FILE")
	if path == "" {
		return cfg, fmt.Errorf("PLUGINS_FILE required")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err = json.Unmarshal(b, &cfg.Plugins); err != nil {
		return cfg, err
	}
	seen := map[string]bool{}
	tokens := map[string]bool{}
	for _, p := range cfg.Plugins {
		u, e := url.Parse(p.URL)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || !catalog.Supported(p.Name) || p.Token == "" || seen[p.Name] || tokens[p.Token] {
			return cfg, fmt.Errorf("invalid plugin config")
		}
		seen[p.Name] = true
		tokens[p.Token] = true
	}
	return cfg, nil
}
