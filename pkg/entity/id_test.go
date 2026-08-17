package entity

import "testing"

func TestKindNameValidate(t *testing.T) {
	if err := KindName("database").Validate(); err != nil {
		t.Errorf("valid kind rejected: %v", err)
	}
	if err := KindName("").Validate(); err == nil {
		t.Error("empty kind accepted")
	}
}

func TestParseResourceID(t *testing.T) {
	cases := []struct {
		in string
		ok bool
	}{
		{"db-42", true},
		{"prod/web", true},
		{"", false},
		{"/web", false},
		{"prod/", false},
	}
	for _, c := range cases {
		got, err := ParseResourceID(c.in)
		if c.ok && (err != nil || got != ResourceID(c.in)) {
			t.Errorf("ParseResourceID(%q): got %q, err %v", c.in, got, err)
		}
		if !c.ok && err == nil {
			t.Errorf("ParseResourceID(%q): accepted", c.in)
		}
	}
}

func TestParseRequestID(t *testing.T) {
	if got, err := ParseRequestID("req-1"); err != nil || got != "req-1" {
		t.Errorf("valid request id rejected: %q %v", got, err)
	}
	if _, err := ParseRequestID(""); err == nil {
		t.Error("empty request id accepted")
	}
}
