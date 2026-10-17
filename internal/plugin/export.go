package plugin

// Export is deliberately a domain projection: no source-room IDs, event IDs, pending
// commands or transport history enter the shared room.
func exportState(name string, state map[string]any) map[string]any {
	out := map[string]any{}
	keys := []string{"title", "description", "owner_id", "revision", "status", "created_at"}
	if name == "monthly_bills" {
		keys = append(keys, "currency", "timezone", "items", "periods", "selected_period")
	}
	for _, k := range keys {
		if v, ok := state[k]; ok {
			out[k] = v
		}
	}
	if name == "checklist" {
		items := []any{}
		for _, raw := range array(state, "items") {
			original := raw.(map[string]any)
			item := map[string]any{}
			for _, key := range []string{"id", "label", "checked", "checked_by", "checked_at"} {
				if v, ok := original[key]; ok {
					item[key] = v
				}
			}
			items = append(items, item)
		}
		out["items"] = items
	}
	return clone(out)
}
