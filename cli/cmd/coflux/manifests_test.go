package main

import (
	"strings"
	"testing"

	"github.com/bitroot/coflux/cli/internal/config"
)

func TestValidateModuleName(t *testing.T) {
	for _, name := range []string{"myapp", "myapp.workflows", "_private", "A1.b2"} {
		if err := validateModuleName(name); err != nil {
			t.Errorf("%q: unexpected error: %v", name, err)
		}
	}

	cases := map[string]string{
		"myapp.*":          "patterns aren't needed",
		"*":                "patterns aren't needed",
		"myapp.py":         "is a file",
		"./myapp/flows.py": "is a file",
		"coflux.toml":      "is a file",
		"README.md":        "is a file",
		"myapp/workflows":  "is a file",
		"1abc":             "is not a module name",
		"my-app":           "is not a module name",
		"myapp..workflows": "is not a module name",
		"myapp.workflows.": "is not a module name",
		"":                 "is not a module name",
	}
	for name, want := range cases {
		err := validateModuleName(name)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: want error containing %q, got %v", name, want, err)
		}
	}
}

func TestResolveModules(t *testing.T) {
	cfg := &config.Config{}
	cfg.Worker.Modules = []string{"configured"}

	if got, err := resolveModules([]string{"given"}, false, cfg); err != nil || strings.Join(got, ",") != "given" {
		t.Errorf("arguments should win: got %v, %v", got, err)
	}
	if got, err := resolveModules(nil, false, cfg); err != nil || strings.Join(got, ",") != "configured" {
		t.Errorf("config should be the fallback: got %v, %v", got, err)
	}
	if got, err := resolveModules(nil, false, &config.Config{}); err != nil || got != nil {
		t.Errorf("nothing configured should mean all (nil): got %v, %v", got, err)
	}
	if got, err := resolveModules(nil, true, cfg); err != nil || got != nil {
		t.Errorf("--all-modules should override config: got %v, %v", got, err)
	}
	if _, err := resolveModules([]string{"given"}, true, cfg); err == nil {
		t.Error("--all-modules with arguments should be an error")
	}
	if _, err := resolveModules([]string{"bad.py"}, false, cfg); err == nil {
		t.Error("an invalid argument should be an error")
	}
	cfg.Worker.Modules = []string{"bad.py"}
	if _, err := resolveModules(nil, false, cfg); err == nil {
		t.Error("an invalid configured module should be an error")
	}
}
