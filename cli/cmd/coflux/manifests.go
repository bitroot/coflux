package main

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/bitroot/coflux/cli/internal/adapter"
	"github.com/bitroot/coflux/cli/internal/api"
	"github.com/bitroot/coflux/cli/internal/config"
	"github.com/spf13/cobra"
)

var manifestsCmd = &cobra.Command{
	Use:   "manifests",
	Short: "Manage manifests",
}

var manifestsAdapter []string
var manifestsInspectWatch bool

func init() {
	manifestsCmd.PersistentFlags().StringSliceVar(&manifestsAdapter, "adapter", nil, "Adapter command (e.g., --adapter python,-m,coflux)")

	manifestsCmd.AddCommand(manifestsDiscoverCmd)
	manifestsCmd.AddCommand(manifestsRegisterCmd)
	manifestsCmd.AddCommand(manifestsArchiveCmd)
	manifestsCmd.AddCommand(manifestsInspectCmd)

	manifestsInspectCmd.Flags().BoolVar(&manifestsInspectWatch, "watch", false, "Watch for changes")

	manifestsDiscoverCmd.Flags().Bool("all-modules", false, "Scan every module in the working directory, ignoring 'worker.modules' in coflux.toml")
	manifestsRegisterCmd.Flags().Bool("all-modules", false, "Scan every module in the working directory, ignoring 'worker.modules' in coflux.toml")
}

var manifestsDiscoverCmd = &cobra.Command{
	Use:   "discover [modules...]",
	Short: "Discover targets from local code",
	Long: `Discover @task and @workflow decorated functions from the specified modules.

This runs the adapter's discovery process and displays the results without
registering anything with the server. A package is scanned recursively. If
no modules are specified, uses 'worker.modules' from coflux.toml, or, when
that isn't set either, every module in the working directory.

Examples:
  coflux manifests discover myapp.workflows myapp.tasks
  coflux manifests discover myapp
  coflux manifests discover -o json myapp.workflows
  coflux manifests discover                              # Use modules from coflux.toml, or all
  coflux manifests discover --all-modules                # All, even if coflux.toml sets modules`,
	RunE: runManifestsDiscover,
}

var manifestsRegisterCmd = &cobra.Command{
	Use:   "register [modules...]",
	Short: "Register modules with the server",
	Long: `Register workflow manifests from the specified modules with the server.

This discovers @task and @workflow decorated functions and registers
their definitions with the Coflux server. A package is scanned
recursively. If no modules are specified, uses 'worker.modules' from
coflux.toml, or, when that isn't set either, every module in the working
directory.

Examples:
  coflux manifests register myapp.workflows myapp.tasks
  coflux manifests register myapp
  coflux manifests register                              # Use modules from coflux.toml, or all
  coflux manifests register --all-modules                # All, even if coflux.toml sets modules`,
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

// resolveModules decides what discovery is pointed at. Arguments win, then
// worker.modules from coflux.toml; with neither the result is nil, and the
// adapter scans the working directory for every module it holds. The
// --all-modules flag asks for that scan explicitly - the one thing leaving
// the arguments out can't express once the config sets a list.
func resolveModules(args []string, allModules bool, cfg *config.Config) ([]string, error) {
	if allModules {
		if len(args) > 0 {
			return nil, fmt.Errorf("--all-modules can't be combined with module arguments")
		}
		return nil, nil
	}
	modules := args
	if len(modules) == 0 {
		modules = cfg.Worker.Modules
	}
	for _, name := range modules {
		if err := validateModuleName(name); err != nil {
			return nil, err
		}
	}
	return modules, nil
}

var moduleNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)

// File extensions a module name can end in only by mistake: a file given
// instead of the name it's imported by, or an unquoted glob the shell
// expanded into the directory listing.
var fileExtensions = map[string]bool{
	"py": true, "toml": true, "md": true, "txt": true, "json": true,
	"yaml": true, "yml": true, "cfg": true, "ini": true, "lock": true,
}

// validateModuleName rejects what the adapter would only fail to import,
// with a hint for the likely mistakes: a file or path, or a pattern,
// where a name already covers its submodules.
func validateModuleName(name string) error {
	looksLikeFile := strings.ContainsAny(name, `/\`) ||
		fileExtensions[strings.ToLower(name[strings.LastIndex(name, ".")+1:])]
	switch {
	case strings.Contains(name, "*"):
		return fmt.Errorf("'%s' is not a module name: a name covers its submodules, so patterns aren't needed (pass no modules to host everything in the working directory)", name)
	case looksLikeFile:
		return fmt.Errorf("'%s' is a file, not a module name: pass the name it's imported by (e.g. myapp.workflows), or no modules to host everything in the working directory", name)
	case moduleNameRe.MatchString(name):
		return nil
	default:
		return fmt.Errorf("'%s' is not a module name (e.g. myapp.workflows)", name)
	}
}

// modulesLabel names a module list in logs and messages.
func modulesLabel(modules []string) string {
	if len(modules) == 0 {
		return "all (working directory)"
	}
	return strings.Join(modules, ", ")
}

func discoverTargets(cmd *cobra.Command, modules []string) (*adapter.DiscoveryManifest, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}

	if len(manifestsAdapter) > 0 {
		cfg.Worker.Adapter = manifestsAdapter
	}
	if len(cfg.Worker.Adapter) == 0 {
		return nil, fmt.Errorf("no adapter configured; use --adapter or add 'worker.adapter' to coflux.toml")
	}

	allModules, _ := cmd.Flags().GetBool("all-modules")
	resolved, err := resolveModules(modules, allModules, cfg)
	if err != nil {
		return nil, err
	}

	cmdAdapter := adapter.NewCommandAdapter(cfg.Worker.Adapter)
	manifest, err := cmdAdapter.Discover(cmd.Context(), resolved)
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

	if isOutput("json") {
		return outputJSON(map[string]any{"targets": workflows})
	}

	if len(workflows) == 0 {
		fmt.Println("No workflows discovered.")
		return nil
	}

	for _, t := range workflows {
		fmt.Printf("%s/%s\n", t.Module, t.Name)
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

	workspaceID, err := ensureWorkspaceID(cmd.Context(), client, workspace)
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

	token, err := resolveToken()
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

	if isOutput("json") {
		manifests, err := client.GetManifests(cmd.Context(), workspaceID)
		if err != nil {
			return fmt.Errorf("failed to get manifests: %w", err)
		}
		return outputJSON(manifests)
	}

	if manifestsInspectWatch {
		return watchTopics(cmd.Context(), getHost(), isSecure(), token, getProject(),
			[]string{"workspaces/" + workspaceID + "/manifests"},
			func(data []map[string]any) []string {
				if data[0] == nil {
					return nil
				}
				return renderManifestLines(data[0])
			},
		)
	}

	manifests, err := client.GetManifests(cmd.Context(), workspaceID)
	if err != nil {
		return fmt.Errorf("failed to get manifests: %w", err)
	}

	for _, line := range renderManifestLines(manifests) {
		fmt.Println(line)
	}
	return nil
}

func renderManifestLines(manifests map[string]any) []string {
	if len(manifests) == 0 {
		return []string{colorDim + "No modules registered." + colorReset}
	}

	modules := make([]string, 0, len(manifests))
	for m := range manifests {
		modules = append(modules, m)
	}
	sort.Strings(modules)

	var lines []string
	for i, module := range modules {
		if i > 0 {
			lines = append(lines, "")
		}
		lines = append(lines, colorBold+module+colorReset)

		targets, ok := manifests[module].(map[string]any)
		if !ok {
			continue
		}
		names := make([]string, 0, len(targets))
		for name := range targets {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			detail := formatTargetDetail(targets[name])
			if detail != "" {
				lines = append(lines, "  "+name+"  "+colorDim+detail+colorReset)
			} else {
				lines = append(lines, "  "+name)
			}
		}
	}
	return lines
}

func formatTargetDetail(raw any) string {
	t, ok := raw.(map[string]any)
	if !ok {
		return ""
	}
	if params, ok := t["parameters"].([]any); ok && len(params) > 0 {
		var names []string
		for _, p := range params {
			if pm, ok := p.(map[string]any); ok {
				if name, ok := pm["name"].(string); ok {
					names = append(names, name)
				}
			}
		}
		return "(" + strings.Join(names, ", ") + ")"
	}
	return ""
}

func buildManifests(manifest *adapter.DiscoveryManifest) map[string]map[string]any {
	// Build manifests map: module -> workflow_name -> definition
	// Only include workflows (not tasks) in manifests
	manifests := make(map[string]map[string]any)

	for _, t := range manifest.Targets {
		if t.Type != "workflow" {
			continue
		}

		if manifests[t.Module] == nil {
			manifests[t.Module] = make(map[string]any)
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

		// Build concurrency (nil if not set)
		var concurrency any
		if t.Concurrency != nil {
			concurrencyMap := map[string]any{
				"limit":  t.Concurrency.Limit,
				"params": t.Concurrency.Params,
			}
			if t.Concurrency.Namespace != nil {
				concurrencyMap["namespace"] = *t.Concurrency.Namespace
			} else {
				concurrencyMap["namespace"] = nil
			}
			concurrency = concurrencyMap
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
				"backoffMin": int64(0),
				"backoffMax": int64(0),
			}
			if t.Retries.Limit != nil {
				retriesMap["limit"] = *t.Retries.Limit
			} else {
				retriesMap["limit"] = nil
			}
			if t.Retries.BackoffMinMs != nil {
				retriesMap["backoffMin"] = *t.Retries.BackoffMinMs
			}
			if t.Retries.BackoffMaxMs != nil {
				retriesMap["backoffMax"] = *t.Retries.BackoffMaxMs
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
			"memo":        t.Memo,
			"concurrency": concurrency,
		}

		manifests[t.Module][t.Name] = def
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
