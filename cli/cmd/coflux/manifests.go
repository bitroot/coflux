package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/bitroot/coflux/cli/internal/adapter"
	"github.com/bitroot/coflux/cli/internal/api"
	"github.com/spf13/cobra"
)

var manifestsCmd = &cobra.Command{
	Use:   "manifests",
	Short: "Manage manifests",
}

var manifestsAdapter []string

func init() {
	manifestsCmd.PersistentFlags().StringSliceVar(&manifestsAdapter, "adapter", nil, "Adapter command (e.g., --adapter python,-m,coflux)")

	manifestsCmd.AddCommand(manifestsDiscoverCmd)
	manifestsCmd.AddCommand(manifestsRegisterCmd)
	manifestsCmd.AddCommand(manifestsArchiveCmd)
	manifestsCmd.AddCommand(manifestsInspectCmd)
}

var manifestsDiscoverCmd = &cobra.Command{
	Use:   "discover <modules...>",
	Short: "Discover targets from local code",
	Long: `Discover @task and @workflow decorated functions from the specified modules.

This runs the adapter's discovery process and displays the results without
registering anything with the server.

Examples:
  coflux manifests discover myapp.workflows myapp.tasks
  coflux manifests discover --json myapp.workflows`,
	Args: cobra.MinimumNArgs(1),
	RunE: runManifestsDiscover,
}

var manifestsRegisterCmd = &cobra.Command{
	Use:   "register <modules...>",
	Short: "Register modules with the server",
	Long: `Register workflow manifests from the specified modules with the server.

This discovers @task and @workflow decorated functions and registers
their definitions with the Coflux server.

Examples:
  coflux manifests register myapp.workflows myapp.tasks`,
	Args: cobra.MinimumNArgs(1),
	RunE: runManifestsRegister,
}

var manifestsArchiveCmd = &cobra.Command{
	Use:   "archive <module>",
	Short: "Archive a module",
	Long: `Archive a module.

Hides the module and its targets from the workspace. The module can be
re-registered later to make it active again.

Example:
  coflux manifests archive myapp.workflows`,
	Args: cobra.ExactArgs(1),
	RunE: runManifestsArchive,
}

var manifestsInspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "List registered modules and targets",
	Long: `List the currently registered modules and their targets for the workspace.

Example:
  coflux manifests inspect`,
	RunE: runManifestsInspect,
}

func discoverTargets(cmd *cobra.Command, modules []string) (*adapter.DiscoveryManifest, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}

	if len(manifestsAdapter) > 0 {
		cfg.Adapter = manifestsAdapter
	}
	if len(cfg.Adapter) == 0 {
		return nil, fmt.Errorf("no adapter configured; use --adapter or add 'adapter' to coflux.toml")
	}

	cmdAdapter := adapter.NewCommandAdapter(cfg.Adapter)
	manifest, err := cmdAdapter.Discover(cmd.Context(), modules)
	if err != nil {
		return nil, fmt.Errorf("discovery failed: %w", err)
	}
	return manifest, nil
}

func runManifestsDiscover(cmd *cobra.Command, args []string) error {
	manifest, err := discoverTargets(cmd, args)
	if err != nil {
		return err
	}

	// Filter to workflows only (matching what register sends to the server)
	var workflows []adapter.TargetDefinition
	for _, t := range manifest.Targets {
		if t.Type == "workflow" {
			workflows = append(workflows, t)
		}
	}

	if getJSON() {
		return outputJSON(map[string]any{"targets": workflows})
	}

	if len(workflows) == 0 {
		fmt.Println("No workflows discovered.")
		return nil
	}

	for _, t := range workflows {
		fmt.Println(t.Name)
	}
	return nil
}

func runManifestsRegister(cmd *cobra.Command, args []string) error {
	workspace, err := requireWorkspace()
	if err != nil {
		return err
	}

	manifest, err := discoverTargets(cmd, args)
	if err != nil {
		return err
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	workspaceID, err := resolveWorkspaceID(cmd.Context(), client, workspace)
	if err != nil {
		return err
	}

	manifests := buildManifests(manifest)

	if err := registerManifests(cmd.Context(), client, workspaceID, manifests); err != nil {
		return fmt.Errorf("failed to register manifests: %w", err)
	}

	fmt.Println("Manifest(s) registered.")
	return nil
}

func runManifestsArchive(cmd *cobra.Command, args []string) error {
	moduleName := args[0]

	workspace, err := requireWorkspace()
	if err != nil {
		return err
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	workspaceID, err := resolveWorkspaceID(cmd.Context(), client, workspace)
	if err != nil {
		return err
	}

	if err := client.ArchiveModule(cmd.Context(), workspaceID, moduleName); err != nil {
		return fmt.Errorf("failed to archive module: %w", err)
	}

	fmt.Printf("Archived module '%s'.\n", moduleName)
	return nil
}

func runManifestsInspect(cmd *cobra.Command, args []string) error {
	workspace, err := requireWorkspace()
	if err != nil {
		return err
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	workspaceID, err := resolveWorkspaceID(cmd.Context(), client, workspace)
	if err != nil {
		return err
	}

	manifests, err := client.GetManifests(cmd.Context(), workspaceID)
	if err != nil {
		return fmt.Errorf("failed to get manifests: %w", err)
	}

	if getJSON() {
		return outputJSON(manifests)
	}

	if len(manifests) == 0 {
		fmt.Println("No modules registered.")
		return nil
	}

	modules := make([]string, 0, len(manifests))
	for m := range manifests {
		modules = append(modules, m)
	}
	sort.Strings(modules)

	for _, module := range modules {
		targets, ok := manifests[module].(map[string]any)
		if !ok {
			fmt.Printf("%s\n", module)
			continue
		}
		names := make([]string, 0, len(targets))
		for name := range targets {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Printf("%s.%s\n", module, name)
		}
	}

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

func registerManifests(ctx context.Context, client *api.Client, workspaceID string, manifests map[string]map[string]any) error {
	return client.RegisterManifests(ctx, workspaceID, manifests)
}
