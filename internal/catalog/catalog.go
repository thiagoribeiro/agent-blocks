package catalog

import (
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

//go:embed assets/*
var assets embed.FS

func Runtime(installed []string) (map[string]any, error) {
	b, err := assets.ReadFile("assets/catalog.json")
	if err != nil {
		return nil, err
	}
	var c map[string]any
	if err = json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	allowed := map[string]bool{}
	for _, p := range installed {
		allowed[p] = true
	}
	plugins := []any{}
	for _, entry := range c["plugins"].([]any) {
		p := entry.(map[string]any)
		name := p["name"].(string)
		if !allowed[name] {
			continue
		}
		p["installed"] = true
		p["status"] = "experimental"
		blocks := p["blocks"].([]any)
		ref := blocks[0].(map[string]any)["schema_ref"].(string)
		view, err := Schema(strings.TrimPrefix(ref, "schemas/"))
		if err != nil {
			return nil, err
		}
		p["view_schema"] = view
		create, err := Schema("requests/" + strings.ReplaceAll(name, "_", "-") + "-create-data-1.0.json")
		if err != nil {
			return nil, err
		}
		p["create_schema"] = create
		commands := p["commands"].([]any)
		hasRefresh := false
		for _, v := range commands {
			if v == "refresh" {
				hasRefresh = true
			}
		}
		if !hasRefresh && name != "agent_status" {
			p["commands"] = append(commands, "refresh")
		}
		if (name == "checklist" || name == "poll" || name == "form_stateful") && os.Getenv("BRIAN_SURFACE_NAMES_ENABLED") != "false" {
			p["commands"] = append(p["commands"].([]any), "rename")
		}
		if name == "checklist" && os.Getenv("BRIAN_SHARING_ENABLED") == "true" {
			p["commands"] = append(p["commands"].([]any), "preview_share", "share", "create_shared", "create_personal", "invite_participant", "remove_participant", "cancel_transfer")
		}
		plugins = append(plugins, p)
	}
	if allowed["monthly_bills"] {
		entry, e := monthlyBillsEntry()
		if e != nil {
			return nil, e
		}
		plugins = append(plugins, entry)
	}
	c["plugins"] = plugins
	message, err := Schema("events/ui-message-1.0.json")
	if err != nil {
		return nil, err
	}
	c["message_schema"] = message
	return c, nil
}

// Schema resolves bundled references, requiring no remote schema discovery.
func Schema(path string) (map[string]any, error) {
	b, err := assets.ReadFile("assets/" + strings.ReplaceAll(path, "/", "__"))
	if err != nil {
		return nil, fmt.Errorf("schema %s: %w", path, err)
	}
	var s map[string]any
	if err = json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	parts := strings.Split(path, "/")
	base := strings.Join(parts[:len(parts)-1], "/")
	var expand func(any) (any, error)
	expand = func(v any) (any, error) {
		switch x := v.(type) {
		case map[string]any:
			if ref, ok := x["$ref"].(string); ok {
				p := base + "/" + ref
				var clean []string
				for _, part := range strings.Split(p, "/") {
					if part == ".." {
						if len(clean) == 0 {
							return nil, fmt.Errorf("bad reference")
						}
						clean = clean[:len(clean)-1]
					} else if part != "." {
						clean = append(clean, part)
					}
				}
				return Schema(strings.Join(clean, "/"))
			}
			for k, a := range x {
				out, e := expand(a)
				if e != nil {
					return nil, e
				}
				x[k] = out
			}
			return x, nil
		case []any:
			for i, a := range x {
				out, e := expand(a)
				if e != nil {
					return nil, e
				}
				x[i] = out
			}
			return x, nil
		default:
			return v, nil
		}
	}
	out, err := expand(s)
	if err != nil {
		return nil, err
	}
	return out.(map[string]any), nil
}
