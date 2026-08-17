package k8slib

import "testing"

func TestParseObjectRef(t *testing.T) {
	ref, err := ParseObjectRef("prod/web")
	if err != nil || ref.Namespace != "prod" || ref.Name != "web" {
		t.Fatalf("ParseObjectRef: %+v %v", ref, err)
	}
	if ref.ID() != "prod/web" {
		t.Fatalf("ID() = %q", ref.ID())
	}
	for _, bad := range []string{"", "web", "/web", "prod/"} {
		if _, err := ParseObjectRef(bad); err == nil {
			t.Errorf("ParseObjectRef(%q): accepted", bad)
		}
	}
}
