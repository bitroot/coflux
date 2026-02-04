package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bitroot/coflux/cli/internal/adapter"
	"github.com/bitroot/coflux/cli/internal/api"
	"github.com/spf13/cobra"
)

var registerCmd = &cobra.Command{
	Use:   "register [modules...]",
	Short: "Register modules with the server",
	Long: `Register workflow manifests from the specified modules with the server.

This discovers @task and @workflow decorated functions and registers
their definitions with the Coflux server.

Modules can be specified as arguments or configured in coflux.toml.

Examples:
  coflux register                            # Uses modules from coflux.toml
  coflux register myapp.workflows myapp.tasks
  coflux register --dry-run                  # Preview without registering`,
	RunE: runRegister,
}

var (
	registerDryRun bool
)

func init() {
	registerCmd.Flags().BoolVar(&registerDryRun, "dry-run", false, "Preview discovered targets without registering")
}

func runRegister(cmd *cobra.Command, args []string) error {
	workspace, err := requireWorkspace()
	if err != nil {
		return err
	}

	// Load config for adapter and modules
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	// Determine modules (args override config)
	modules := args
	if len(modules) == 0 {
		modules = cfg.Modules
	}
	if len(modules) == 0 {
		return fmt.Errorf("no modules specified; add 'modules' to coflux.toml or pass as arguments")
	}

	// Check adapter is configured
	if len(cfg.Adapter) == 0 {
		return fmt.Errorf("no adapter configured; run 'coflux setup' or add 'adapter' to coflux.toml")
	}

	// Create adapter and run discovery
	cmdAdapter := adapter.NewCommandAdapter(cfg.Adapter)
	manifest, err := cmdAdapter.Discover(cmd.Context(), modules)
	if err != nil {
		return fmt.Errorf("discovery failed: %w", err)
	}

	if registerDryRun {
		// Just print discovered targets
		return printDiscoveredTargets(manifest)
	}

	// Create client with resolved token
	client, err := newClient()
	if err != nil {
		return err
	}

	// Register with server
	manifests := buildManifests(manifest)

	if err := registerManifests(cmd.Context(), client, workspace, manifests); err != nil {
		return fmt.Errorf("failed to register manifests: %w", err)
	}

	fmt.Println("Manifest(s) registered.")
	return nil
}

func printDiscoveredTargets(manifest *adapter.DiscoveryManifest) error {
	output, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}

func buildManifests(manifest *adapter.DiscoveryManifest) map[string]map[string]any {
	// Build manifests map: module -> workflow_name -> definition
	// Only include workflows (not tasks) in manifests
	manifests := make(map[string]map[string]any)

	for _, t := range manifest.Targets {
		if t.Type != "workflow" {
			continue
		}

		module, name := splitTarget(t.Name)
		if manifests[module] == nil {
			manifests[module] = make(map[string]any)
		}

		// Build waitFor as list (empty if nil)
		waitFor := []int{}
		if arr, ok := t.WaitFor.([]any); ok {
			for _, v := range arr {
				if n, ok := v.(float64); ok {
					waitFor = append(waitFor, int(n))
				}
			}
		}

		// Build cache (nil if not set)
		var cache any
		if t.Cache != nil {
			cacheMap := map[string]any{
				"params": t.Cache.Params,
			}
			if t.Cache.MaxAgeMs != nil {
				cacheMap["maxAge"] = *t.Cache.MaxAgeMs
			} else {
				cacheMap["maxAge"] = nil
			}
			if t.Cache.Namespace != nil {
				cacheMap["namespace"] = *t.Cache.Namespace
			} else {
				cacheMap["namespace"] = nil
			}
			if t.Cache.Version != nil {
				cacheMap["version"] = *t.Cache.Version
			} else {
				cacheMap["version"] = nil
			}
			cache = cacheMap
		}

		// Build defer (nil if not set)
		var defer_ any
		if t.Defer != nil {
			defer_ = map[string]any{
				"params": t.Defer.Params,
			}
		}

		// Delay is already in milliseconds from the adapter (0 if not set - server requires integer, not nil)
		delay := 0
		if t.Delay != nil {
			delay = int(*t.Delay)
		}

		// Build retries (nil if not set, use 0 for delay values like Python does)
		var retries any
		if t.Retries != nil {
			retriesMap := map[string]any{
				"delayMin": int64(0),
				"delayMax": int64(0),
			}
			if t.Retries.Limit != nil {
				retriesMap["limit"] = *t.Retries.Limit
			} else {
				retriesMap["limit"] = nil
			}
			if t.Retries.DelayMinMs != nil {
				retriesMap["delayMin"] = *t.Retries.DelayMinMs
			}
			if t.Retries.DelayMaxMs != nil {
				retriesMap["delayMax"] = *t.Retries.DelayMaxMs
			}
			retries = retriesMap
		}

		// Build requires (nil if not set, like Python does)
		var requires any
		if len(t.Requires) > 0 {
			requires = t.Requires
		}

		// Build instruction (nil if not set)
		var instruction any
		if t.Instruction != nil {
			instruction = *t.Instruction
		}

		def := map[string]any{
			"parameters":  buildParameters(t.Parameters),
			"waitFor":     waitFor,
			"cache":       cache,
			"defer":       defer_,
			"delay":       delay,
			"retries":     retries,
			"recurrent":   t.Recurrent,
			"requires":    requires,
			"instruction": instruction,
		}

		manifests[module][name] = def
	}

	return manifests
}

func buildParameters(params []adapter.Parameter) []map[string]any {
	result := make([]map[string]any, len(params))
	for i, p := range params {
		param := map[string]any{
			"name": p.Name,
		}
		if p.Annotation != nil {
			param["annotation"] = *p.Annotation
		}
		if p.Default != nil {
			param["default"] = *p.Default
		}
		result[i] = param
	}
	return result
}

func registerManifests(ctx context.Context, client *api.Client, workspace string, manifests map[string]map[string]any) error {
	return client.RegisterManifests(ctx, workspace, manifests)
}
