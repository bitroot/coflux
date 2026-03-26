package version

import "testing"

func TestAPIVersion(t *testing.T) {
	tests := []struct {
		version  string
		expected string
	}{
		{"dev", "dev"},
		{"", "dev"},
		{"0.9.0", "0.9"},
		{"0.9.1", "0.9"},
		{"0.10.0", "0.10"},
		{"0.1.0", "0.1"},
		{"1.0.0", "1"},
		{"1.2.3", "1"},
		{"2.0.0", "2"},
		{"abc", "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			original := Version
			Version = tt.version
			defer func() { Version = original }()

			got := APIVersion()
			if got != tt.expected {
				t.Errorf("APIVersion() with Version=%q: got %q, want %q", tt.version, got, tt.expected)
			}
		})
	}
}
