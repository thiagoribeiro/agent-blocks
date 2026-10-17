package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type PluginHandler interface {
	Request(context.Context, string, string, []byte) (int, []byte, error)
}
type remotePlugin struct {
	cfg    PluginConfig
	client *http.Client
}

func (p *remotePlugin) Request(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(p.cfg.URL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+p.cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	res, err := p.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	return res.StatusCode, b, err
}

type Registry struct {
	handlers map[string]PluginHandler
	configs  map[string]PluginConfig
}

func newRegistry() *Registry {
	return &Registry{handlers: map[string]PluginHandler{}, configs: map[string]PluginConfig{}}
}
func (r *Registry) Register(p PluginConfig) {
	r.configs[p.Name] = p
	r.handlers[p.Name] = &remotePlugin{cfg: p, client: &http.Client{Timeout: 20 * time.Second}}
}
func (r *Registry) Get(name string) (PluginHandler, bool) { p, ok := r.handlers[name]; return p, ok }
func (r *Registry) Names() []string {
	names := []string{}
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}
func (r *Registry) Fetch(ctx context.Context, name, path string, v any) error {
	p, ok := r.Get(name)
	if !ok {
		return fmt.Errorf("unknown plugin")
	}
	code, b, err := p.Request(ctx, "GET", path, nil)
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("plugin returned %d", code)
	}
	return json.Unmarshal(b, v)
}
