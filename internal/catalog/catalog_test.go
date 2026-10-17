package catalog

import "testing"

func TestInstalledOnly(t *testing.T) {
	c, err := Runtime([]string{"approval", "workflow", "form_stateful", "poll", "checklist"})
	if err != nil {
		t.Fatal(err)
	}
	ps := c["plugins"].([]any)
	if len(ps) != 5 {
		t.Fatalf("catalog has %d entries", len(ps))
	}
	seen := map[string]bool{}
	for _, raw := range ps {
		p := raw.(map[string]any)
		name := p["name"].(string)
		if seen[name] {
			t.Fatal("duplicate plugin", name)
		}
		seen[name] = true
		for _, cmd := range p["commands"].([]any) {
			if name == "workflow" && cmd != "refresh" {
				t.Fatal("unsupported workflow command", cmd)
			}
		}
	}
}
