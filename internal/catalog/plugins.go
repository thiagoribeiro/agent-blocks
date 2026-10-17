package catalog

// Plugins lists every plugin this build can run, as configured by BRIAN_PLUGIN
// and accepted in the router's plugin registry.
var Plugins = []string{"approval", "workflow", "form_stateful", "checklist", "poll", "agent_status", "monthly_bills"}

// Supported reports whether name is one of Plugins.
func Supported(name string) bool {
	for _, p := range Plugins {
		if p == name {
			return true
		}
	}
	return false
}
