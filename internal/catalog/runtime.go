package catalog

import (
	"embed"
	"encoding/json"
	"os"
)

// These are application/plugin schemas; the bundled BRIAN specification stays immutable.
//
//go:embed runtime_assets/*
var runtimeAssets embed.FS

func runtimeSchema(name string) (map[string]any, error) {
	b, err := runtimeAssets.ReadFile("runtime_assets/" + name)
	if err != nil {
		return nil, err
	}
	var s map[string]any
	err = json.Unmarshal(b, &s)
	return s, err
}
func monthlyBillsEntry() (map[string]any, error) {
	view, err := runtimeSchema("monthly-bills-view.json")
	if err != nil {
		return nil, err
	}
	create, err := runtimeSchema("monthly-bills-create.json")
	if err != nil {
		return nil, err
	}
	commands := []any{"create_personal", "add_bill", "update_bill", "archive_bill", "open_period", "confirm_amount", "record_payment", "cancel_entry", "archive", "refresh"}
	if os.Getenv("BRIAN_SURFACE_NAMES_ENABLED") != "false" {
		commands = append(commands, "rename")
	}
	if os.Getenv("BRIAN_SHARING_ENABLED") == "true" {
		commands = append(commands, "create_shared", "preview_share", "share", "invite_participant", "remove_participant", "cancel_transfer")
	}
	return map[string]any{"name": "monthly_bills", "version": "1.0", "installed": true, "status": "experimental", "purpose": "Track monthly bills, suggested amounts and confirmed payments; never moves money.", "description": "Household bills", "commands": commands, "view_schema": view, "create_schema": create, "instance_creation": map[string]any{"mode": "plugin_service_required", "operation": "monthly_bills.create"}, "surfaces": map[string]any{"default": "pinned", "auto_pin_when": []any{"open"}, "auto_unpin_when": []any{"archived"}, "supports_user_pin": true, "supports_focus_view": true}, "fallback_required": true}, nil
}
