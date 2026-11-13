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

func TestEmailTriageIsProducerOnlyAndClosedToTheLLM(t *testing.T) {
	c, err := Runtime([]string{"email_triage"})
	if err != nil {
		t.Fatal(err)
	}
	ps := c["plugins"].([]any)
	if len(ps) != 1 {
		t.Fatalf("catalog has %d entries", len(ps))
	}
	p := ps[0].(map[string]any)
	if p["name"] != "email_triage" || p["llm_access"] != "none" || len(p["delegable_commands"].([]any)) != 0 ||
		p["instance_creation"].(map[string]any)["mode"] != "producer_only" || p["view_schema"] == nil || p["create_schema"] == nil {
		t.Fatal(p)
	}
	if c, _ := Runtime([]string{"approval"}); len(c["plugins"].([]any)) != 1 {
		t.Fatal("email_triage listed without being installed")
	}
}
