package provision

import "testing"

func TestSupportsSchedules(t *testing.T) {
	for _, tc := range []struct {
		version string
		want    bool
		bad     bool
	}{
		{"2.11.17", false, false},
		{"2.12.0", true, false},
		{"2.15.0", true, false},
		{"v3.0.0", true, false},
		{"", false, true},
		{"unknown", false, true},
	} {
		got, err := supportsSchedules(tc.version)
		if (err != nil) != tc.bad || got != tc.want {
			t.Errorf("version %q: supported=%v err=%v", tc.version, got, err)
		}
	}
}
