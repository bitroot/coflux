package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/bitroot/coflux/cli/internal/api"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var poolsCmd = &cobra.Command{
	Use:   "pools",
	Short: "Manage pools",
}

func init() {
	poolsCmd.AddCommand(poolsListCmd)
	poolsCmd.AddCommand(poolsGetCmd)
	poolsCmd.AddCommand(poolsCreateCmd)
	poolsCmd.AddCommand(poolsUpdateCmd)
	poolsCmd.AddCommand(poolsDeleteCmd)
	poolsCmd.AddCommand(poolsLaunchesCmd)
	poolsCmd.AddCommand(poolsDisableCmd)
	poolsCmd.AddCommand(poolsEnableCmd)
	poolsCmd.AddCommand(poolsExportCmd)
	poolsCmd.AddCommand(poolsImportCmd)
}

// pools list
var poolsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List pools",
	RunE:  runPoolsList,
}

func runPoolsList(cmd *cobra.Command, args []string) error {
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

	pools, err := client.GetPools(cmd.Context(), workspaceID)
	if err != nil {
		return err
	}

	if isOutput("json") {
		return outputJSON(pools)
	}

	if len(pools) == 0 {
		fmt.Println("No pools found.")
		return nil
	}

	var rows [][]string
	for name, pool := range pools {
		launcher := ""
		if l, ok := pool["launcher"].(map[string]any); ok {
			launcher = getString(l, "type")
		}
		modules := ""
		if m, ok := pool["modules"].([]any); ok {
			var mods []string
			for _, mod := range m {
				if s, ok := mod.(string); ok {
					mods = append(mods, s)
				}
			}
			modules = strings.Join(mods, ",")
		}
		provides := encodeProvides(pool["provides"])
		accepts := encodeProvides(pool["accepts"])
		rows = append(rows, []string{name, launcher, modules, provides, accepts})
	}

	printTable([]string{"Name", "Launcher", "Modules", "Provides", "Accepts"}, rows)
	return nil
}

// pools get
var poolsGetCmd = &cobra.Command{
	Use:   "get <name>",
	Short: "Get pool configuration",
	Args:  cobra.ExactArgs(1),
	RunE:  runPoolsGet,
}

func runPoolsGet(cmd *cobra.Command, args []string) error {
	name := args[0]

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

	pool, err := client.GetPool(cmd.Context(), workspaceID, name)
	if err != nil {
		return fmt.Errorf("failed to get pool '%s': %w", name, err)
	}

	if isOutput("json") {
		return outputJSON(pool)
	}

	// Modules
	if modules, ok := pool["modules"].([]any); ok && len(modules) > 0 {
		var mods []string
		for _, m := range modules {
			if s, ok := m.(string); ok {
				mods = append(mods, s)
			}
		}
		fmt.Printf("Modules: %s\n", strings.Join(mods, ", "))
	}

	// Provides
	if provides := encodeProvides(pool["provides"]); provides != "" {
		fmt.Printf("Provides: %s\n", provides)
	}

	// Accepts
	if accepts := encodeProvides(pool["accepts"]); accepts != "" {
		fmt.Printf("Accepts: %s\n", accepts)
	}

	// Launcher
	if launcher, ok := pool["launcher"].(map[string]any); ok {
		fmt.Printf("Launcher: %s\n", getString(launcher, "type"))
		if image := getString(launcher, "image"); image != "" {
			fmt.Printf("Image: %s\n", image)
		}
		if dir := getString(launcher, "directory"); dir != "" {
			fmt.Printf("Directory: %s\n", dir)
		}
		if dockerHost := getString(launcher, "dockerHost"); dockerHost != "" {
			fmt.Printf("Docker host: %s\n", dockerHost)
		}
		if ns := getString(launcher, "namespace"); ns != "" {
			fmt.Printf("Namespace: %s\n", ns)
		}
		if apiServer := getString(launcher, "apiServer"); apiServer != "" {
			insecure, _ := launcher["insecure"].(bool)
			if insecure {
				fmt.Printf("API server: %s (no verify)\n", apiServer)
			} else {
				fmt.Printf("API server: %s\n", apiServer)
			}
		} else if insecure, ok := launcher["insecure"].(bool); ok && insecure {
			fmt.Printf("API server: (no verify)\n")
		}
		if sa := getString(launcher, "serviceAccount"); sa != "" {
			fmt.Printf("Service account: %s\n", sa)
		}
		if policy := getString(launcher, "imagePullPolicy"); policy != "" {
			fmt.Printf("Image pull policy: %s\n", policy)
		}
		printServerHost(launcher)
		if adapter := getStringSlice(launcher, "adapter"); len(adapter) > 0 {
			fmt.Printf("Adapter: %s\n", strings.Join(adapter, " "))
		}
		if concurrency := getFloat64(launcher, "concurrency"); concurrency > 0 {
			fmt.Printf("Concurrency: %d\n", int(concurrency))
		}
		if env, ok := launcher["env"].(map[string]any); ok && len(env) > 0 {
			fmt.Printf("Environment:\n")
			for k, v := range env {
				fmt.Printf("  %s=%s\n", k, v)
			}
		}
	}

	return nil
}

// pools launches
var poolsLaunchesWatch bool

var poolsLaunchesCmd = &cobra.Command{
	Use:   "launches <pool> [worker-id]",
	Short: "Show pool launches",
	Long: `Show launches for a pool. Without a worker ID, lists all workers.
With a worker ID, shows details for that specific launch.

Use --watch to stream live updates.`,
	Args: cobra.RangeArgs(1, 2),
	RunE: runPoolsLaunches,
}

func init() {
	poolsLaunchesCmd.Flags().BoolVar(&poolsLaunchesWatch, "watch", false, "Watch for changes")
}

func runPoolsLaunches(cmd *cobra.Command, args []string) error {
	poolName := args[0]

	if len(args) == 2 {
		return runPoolsLaunchDetail(cmd, poolName, args[1])
	}

	if poolsLaunchesWatch {
		return runPoolsLaunchFollow(cmd, poolName)
	}

	return runPoolsLaunchList(cmd, poolName)
}

func runPoolsLaunchList(cmd *cobra.Command, poolName string) error {
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

	data, err := client.CaptureTopic(cmd.Context(), "workspaces/"+workspaceID+"/pools/"+poolName)
	if err != nil {
		return fmt.Errorf("pool '%s' not found", poolName)
	}

	workers, _ := data["workers"].(map[string]any)

	if isOutput("json") {
		return outputJSON(workers)
	}

	for _, line := range renderWorkerLines(data) {
		fmt.Println(line)
	}
	return nil
}

func runPoolsLaunchFollow(cmd *cobra.Command, poolName string) error {
	workspace, err := requireWorkspace()
	if err != nil {
		return err
	}

	apiClient, err := newClient()
	if err != nil {
		return err
	}

	workspaceID, err := resolveWorkspaceID(cmd.Context(), apiClient, workspace)
	if err != nil {
		return err
	}

	token, err := resolveToken()
	if err != nil {
		return err
	}

	return watchTopics(cmd.Context(), getHost(), isSecure(), token, getProject(),
		[]string{"workspaces/" + workspaceID + "/pools/" + poolName},
		func(data []map[string]any) []string {
			if data[0] == nil {
				return nil
			}
			return renderWorkerLines(data[0])
		},
	)
}

type workerRow struct {
	id     string
	status string
	error  string
	ts     int64
}

func buildWorkerRows(workers map[string]any) []workerRow {
	var rows []workerRow
	for id, raw := range workers {
		w, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		status, errMsg, ts := workerStatus(w)
		rows = append(rows, workerRow{id: id, status: status, error: errMsg, ts: ts})
	}
	// Sort by timestamp descending (most recent first)
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].ts > rows[j].ts
	})
	return rows
}

func renderWorkerLines(data map[string]any) []string {
	workers, _ := data["workers"].(map[string]any)
	if workers == nil {
		return []string{colorDim + "No launches." + colorReset}
	}

	rows := buildWorkerRows(workers)

	if len(rows) == 0 {
		return []string{colorDim + "No launches." + colorReset}
	}

	var lines []string
	for _, r := range rows {
		ts := colorDim + formatMillis(r.ts) + colorReset
		statusStr := formatWorkerStatus(r.status)
		line := fmt.Sprintf("%s  %s  %s", ts, r.id, statusStr)
		if r.error != "" {
			line += "  " + r.error
		}
		lines = append(lines, line)
	}
	return lines
}

func workerStatus(w map[string]any) (status, errMsg string, ts int64) {
	startingAt := getFloat64(w, "startingAt")
	startedAt := getFloat64(w, "startedAt")
	deactivatedAt := getFloat64(w, "deactivatedAt")
	stoppingAt := getFloat64(w, "stoppingAt")
	startError, _ := w["startError"].(string)
	workerError, _ := w["error"].(string)
	connected, _ := w["connected"].(bool)

	if deactivatedAt > 0 {
		ts = int64(deactivatedAt)
		if workerError != "" {
			return "failed", workerError, ts
		}
		if startError != "" {
			return "failed", startError, ts
		}
		return "stopped", "", ts
	}
	if stoppingAt > 0 {
		return "stopping", "", int64(stoppingAt)
	}
	if startedAt > 0 {
		ts = int64(startedAt)
		if connected {
			return "running", "", ts
		}
		return "started", "", ts
	}
	if startError != "" {
		return "failed", startError, int64(startingAt)
	}
	if startingAt > 0 {
		return "starting", "", int64(startingAt)
	}
	return "unknown", "", 0
}

func formatWorkerStatus(status string) string {
	switch status {
	case "running":
		return colorGreen + "◆ Running" + colorReset
	case "started":
		return colorBlue + "◆ Started" + colorReset
	case "starting":
		return colorDim + "◇ Starting" + colorReset
	case "stopping":
		return colorYellow + "◆ Stopping" + colorReset
	case "stopped":
		return colorDim + "◇ Stopped" + colorReset
	case "failed":
		return colorRed + "✗ Failed" + colorReset
	default:
		return colorDim + "◇ Unknown" + colorReset
	}
}

func runPoolsLaunchDetail(cmd *cobra.Command, poolName, workerID string) error {
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

	data, err := client.CaptureTopic(cmd.Context(), "workspaces/"+workspaceID+"/pools/"+poolName)
	if err != nil {
		return fmt.Errorf("pool '%s' not found", poolName)
	}

	workers, _ := data["workers"].(map[string]any)
	if workers == nil {
		return fmt.Errorf("worker '%s' not found", workerID)
	}

	w, ok := workers[workerID].(map[string]any)
	if !ok {
		return fmt.Errorf("worker '%s' not found", workerID)
	}

	if isOutput("json") {
		return outputJSON(w)
	}

	// Text output
	status, errMsg, _ := workerStatus(w)
	statusLine := formatWorkerStatus(status)
	if errMsg != "" {
		statusLine += "  " + errMsg
	}
	fmt.Printf("Status:         %s\n", statusLine)

	if v := getFloat64(w, "startingAt"); v > 0 {
		fmt.Printf("Starting at:    %s\n", formatMillis(int64(v)))
	}
	if v := getFloat64(w, "startedAt"); v > 0 {
		fmt.Printf("Started at:     %s\n", formatMillis(int64(v)))
	}
	if v := getFloat64(w, "stoppingAt"); v > 0 {
		fmt.Printf("Stopping at:    %s\n", formatMillis(int64(v)))
	}
	if v := getFloat64(w, "stoppedAt"); v > 0 {
		fmt.Printf("Stopped at:     %s\n", formatMillis(int64(v)))
	}
	if v := getFloat64(w, "deactivatedAt"); v > 0 {
		fmt.Printf("Deactivated at: %s\n", formatMillis(int64(v)))
	}
	if connected, ok := w["connected"].(bool); ok {
		fmt.Printf("Connected:      %v\n", connected)
	}

	if logs, _ := w["logs"].(string); logs != "" {
		fmt.Printf("\nLogs:\n%s%s%s\n", colorDim, logs, colorReset)
	}

	return nil
}

func formatMillis(ms int64) string {
	t := time.Unix(ms/1000, (ms%1000)*int64(time.Millisecond)).UTC()
	return t.Format("2006-01-02 15:04:05 UTC")
}

func getFloat64(m map[string]any, key string) float64 {
	if v, ok := m[key]; ok {
		if f, ok := v.(float64); ok {
			return f
		}
	}
	return 0
}

func getStringSlice(m map[string]any, key string) []string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	if arr, ok := v.([]any); ok {
		result := make([]string, 0, len(arr))
		for _, item := range arr {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	}
	return nil
}

// pools create

var poolsCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a pool",
	Long: `Create a new pool with the specified launcher type and configuration.

Use --type to specify the launcher type (kubernetes, docker, or process).
Use --set to set field values. Values are parsed as JSON if valid, otherwise
treated as strings.

Examples:
  coflux pools create my-pool --type kubernetes --set image=foo:latest --set namespace=default
  coflux pools create my-pool --type docker --set image=myapp:v1 --modules mod1,mod2
  coflux pools create my-pool --type process --set directory=/app --set concurrency=5`,
	Args: cobra.ExactArgs(1),
	RunE: runPoolsCreate,
}

// pools update

var poolsUpdateCmd = &cobra.Command{
	Use:   "update <name>",
	Short: "Update a pool",
	Long: `Update an existing pool's configuration.

Use --set to set field values and --unset to clear them. Values are parsed as
JSON if valid, otherwise treated as strings.

Examples:
  coflux pools update my-pool --set image=foo:latest
  coflux pools update my-pool --set insecure=true --set concurrency=5
  coflux pools update my-pool --set env.MY_VAR=hello --unset env.OLD_VAR
  coflux pools update my-pool --unset imagePullPolicy
  coflux pools update my-pool --modules mod1,mod2`,
	Args: cobra.ExactArgs(1),
	RunE: runPoolsUpdate,
}

func init() {
	// pools create flags
	poolsCreateCmd.Flags().String("type", "", "Launcher type (kubernetes, docker, process)")
	_ = poolsCreateCmd.MarkFlagRequired("type")
	poolsCreateCmd.Flags().StringArray("set", nil, "Set a field value (key=value)")
	poolsCreateCmd.Flags().StringSlice("modules", nil, "Modules to be hosted")
	poolsCreateCmd.Flags().StringSlice("provides", nil, "Features that workers provide")
	poolsCreateCmd.Flags().StringSlice("accepts", nil, "Tags that executions must have")

	// pools update flags
	poolsUpdateCmd.Flags().StringArray("set", nil, "Set a field value (key=value)")
	poolsUpdateCmd.Flags().StringArray("unset", nil, "Unset a field")
	poolsUpdateCmd.Flags().StringSlice("modules", nil, "Modules to be hosted")
	poolsUpdateCmd.Flags().StringSlice("provides", nil, "Features that workers provide")
	poolsUpdateCmd.Flags().StringSlice("accepts", nil, "Tags that executions must have")
	poolsUpdateCmd.Flags().Bool("no-provides", false, "Clear provides")
	poolsUpdateCmd.Flags().Bool("no-accepts", false, "Clear accepts")
}

// parseSetValue parses the value part of a --set flag.
// Tries JSON parsing first, falls back to string.
func parseSetValue(s string) any {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err == nil {
		return v
	}
	return s
}

// poolTopLevelFields lists field names that are pool-level (not launcher-level).
var poolTopLevelFields = map[string]bool{
	"modules":  true,
	"provides": true,
	"accepts":  true,
}

// launcherFields lists valid launcher field names.
var launcherFields = map[string]bool{
	"image": true, "dockerHost": true, "directory": true,
	"namespace": true, "serviceAccount": true, "apiServer": true,
	"token": true, "caCert": true, "insecure": true,
	"imagePullPolicy": true, "nodeSelector": true, "tolerations": true,
	"imagePullSecrets": true, "hostAliases": true, "resources": true,
	"labels": true, "annotations": true, "activeDeadlineSeconds": true,
	"volumes": true, "volumeMounts": true,
	"serverHost": true, "serverSecure": true, "adapter": true,
	"concurrency": true, "env": true,
}

// mapSubkeyFields lists launcher fields that support dotted sub-key access
// (e.g. labels.team=ml, annotations.prometheus.io/scrape=true).
var mapSubkeyFields = map[string]bool{
	"env":          true,
	"labels":       true,
	"annotations":  true,
	"nodeSelector": true,
	"resources":    true,
}

// isValidFieldName checks if a field name (possibly with dotted prefix) is valid.
func isValidFieldName(name string) bool {
	if poolTopLevelFields[name] || launcherFields[name] {
		return true
	}
	if base, _, ok := strings.Cut(name, "."); ok {
		return mapSubkeyFields[base]
	}
	return false
}

// poolFieldOp represents a single set or unset operation.
type poolFieldOp struct {
	action string // "set" or "unset"
	key    string
	value  any // only for "set"
}

// collectFieldOps builds an ordered list of field operations from os.Args,
// processing convenience flags (--modules, --provides, --accepts, --no-provides,
// --no-accepts) alongside --set/--unset in the order they appear on the command line.
func collectFieldOps(cmd *cobra.Command) ([]poolFieldOp, error) {
	var ops []poolFieldOp

	// Build a map from flag to its parsed values for quick lookup
	setValues, _ := cmd.Flags().GetStringArray("set")
	unsetValues, _ := cmd.Flags().GetStringArray("unset")
	setIdx := 0
	unsetIdx := 0

	modulesValues, _ := cmd.Flags().GetStringSlice("modules")
	providesValues, _ := cmd.Flags().GetStringSlice("provides")
	acceptsValues, _ := cmd.Flags().GetStringSlice("accepts")

	// Walk os.Args to determine command-line order of flags
	args := os.Args
	for i := 0; i < len(args); i++ {
		arg := args[i]

		// Normalize: handle --flag=value and --flag value forms
		flagName := ""
		switch {
		case strings.HasPrefix(arg, "--set="):
			flagName = "set"
		case arg == "--set" && i+1 < len(args):
			flagName = "set"
		case strings.HasPrefix(arg, "--unset="):
			flagName = "unset"
		case arg == "--unset" && i+1 < len(args):
			flagName = "unset"
		case strings.HasPrefix(arg, "--modules=") || arg == "--modules":
			flagName = "modules"
		case strings.HasPrefix(arg, "--provides=") || arg == "--provides":
			flagName = "provides"
		case strings.HasPrefix(arg, "--accepts=") || arg == "--accepts":
			flagName = "accepts"
		case arg == "--no-provides":
			flagName = "no-provides"
		case arg == "--no-accepts":
			flagName = "no-accepts"
		default:
			continue
		}

		switch flagName {
		case "set":
			if setIdx < len(setValues) {
				kv := setValues[setIdx]
				setIdx++
				key, val, hasVal := strings.Cut(kv, "=")
				if !hasVal {
					return nil, fmt.Errorf("invalid --set value %q: must be key=value", kv)
				}
				if !isValidFieldName(key) {
					return nil, fmt.Errorf("unknown field %q", key)
				}
				ops = append(ops, poolFieldOp{action: "set", key: key, value: parseSetValue(val)})
			}
			// Skip next arg if it was --set value (not --set=value)
			if !strings.Contains(arg, "=") {
				i++
			}
		case "unset":
			if unsetIdx < len(unsetValues) {
				key := unsetValues[unsetIdx]
				unsetIdx++
				if !isValidFieldName(key) {
					return nil, fmt.Errorf("unknown field %q", key)
				}
				ops = append(ops, poolFieldOp{action: "unset", key: key})
			}
			if !strings.Contains(arg, "=") {
				i++
			}
		case "modules":
			ops = append(ops, poolFieldOp{action: "set", key: "modules", value: toAnySlice(modulesValues)})
			if !strings.Contains(arg, "=") {
				i++
			}
		case "provides":
			ops = append(ops, poolFieldOp{action: "set", key: "provides", value: parseProvides(providesValues)})
			if !strings.Contains(arg, "=") {
				i++
			}
		case "accepts":
			ops = append(ops, poolFieldOp{action: "set", key: "accepts", value: parseProvides(acceptsValues)})
			if !strings.Contains(arg, "=") {
				i++
			}
		case "no-provides":
			ops = append(ops, poolFieldOp{action: "unset", key: "provides"})
		case "no-accepts":
			ops = append(ops, poolFieldOp{action: "unset", key: "accepts"})
		}
	}

	return ops, nil
}

// toAnySlice converts []string to []any for JSON serialization.
func toAnySlice(ss []string) []any {
	result := make([]any, len(ss))
	for i, s := range ss {
		result[i] = s
	}
	return result
}

// applyOps applies field operations to build pool and launcher maps.
// Returns the pool map (which may contain a "launcher" sub-map).
func applyOps(ops []poolFieldOp, includeLauncher bool) map[string]any {
	pool := make(map[string]any)
	launcher := make(map[string]any)
	hasLauncher := false

	for _, op := range ops {
		base, sub, hasSub := strings.Cut(op.key, ".")

		if poolTopLevelFields[op.key] {
			// Pool-level field
			if op.action == "set" {
				pool[op.key] = op.value
			} else {
				pool[op.key] = nil
			}
		} else if hasSub && mapSubkeyFields[base] {
			// Dotted sub-key (e.g. env.KEY, labels.team, resources.cpu)
			hasLauncher = true
			m, ok := launcher[base].(map[string]any)
			if !ok {
				m = make(map[string]any)
			}
			if op.action == "set" {
				m[sub] = op.value
			} else {
				m[sub] = nil
			}
			launcher[base] = m
		} else {
			// Launcher-level field
			hasLauncher = true
			if op.action == "set" {
				launcher[op.key] = op.value
			} else {
				launcher[op.key] = nil
			}
		}
	}

	if hasLauncher && includeLauncher {
		pool["launcher"] = launcher
	}

	return pool
}

func runPoolsCreate(cmd *cobra.Command, args []string) error {
	name := args[0]

	workspace, err := requireWorkspace()
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

	launcherType, _ := cmd.Flags().GetString("type")

	// Collect and validate field operations
	ops, err := collectFieldOps(cmd)
	if err != nil {
		return err
	}

	// Build the pool definition
	pool := applyOps(ops, true)

	// Ensure launcher exists and set type
	launcher, ok := pool["launcher"].(map[string]any)
	if !ok {
		launcher = make(map[string]any)
	}
	launcher["type"] = launcherType
	pool["launcher"] = launcher

	if err := client.CreatePool(cmd.Context(), workspaceID, name, pool); err != nil {
		return err
	}

	fmt.Printf("Created pool '%s'.\n", name)
	return nil
}

func runPoolsUpdate(cmd *cobra.Command, args []string) error {
	name := args[0]

	workspace, err := requireWorkspace()
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

	// Collect and validate field operations
	ops, err := collectFieldOps(cmd)
	if err != nil {
		return err
	}

	if len(ops) == 0 {
		return fmt.Errorf("no changes specified; use --set, --unset, or convenience flags")
	}

	// Build the patch (only includes fields that were explicitly set/unset)
	pool := applyOps(ops, true)

	if err := client.UpdatePool(cmd.Context(), workspaceID, name, pool); err != nil {
		return err
	}

	fmt.Printf("Updated pool '%s'.\n", name)
	return nil
}

// Ensure pflag is used (for the import).
var _ pflag.Value

// pools delete
var poolsDeleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a pool",
	Args:  cobra.ExactArgs(1),
	RunE:  runPoolsDelete,
}

func runPoolsDelete(cmd *cobra.Command, args []string) error {
	name := args[0]

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

	if err := client.UpdatePool(cmd.Context(), workspaceID, name, nil); err != nil {
		return err
	}

	fmt.Printf("Deleted pool '%s'.\n", name)
	return nil
}

// pools disable
var poolsDisableCmd = &cobra.Command{
	Use:   "disable <name>",
	Short: "Disable a pool",
	Long:  `Disable a pool. New executions won't be assigned to this pool's workers, and no new workers will be launched. Existing workers will drain.`,
	Args:  cobra.ExactArgs(1),
	RunE:  runPoolsDisable,
}

func runPoolsDisable(cmd *cobra.Command, args []string) error {
	name := args[0]

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

	if err := client.DisablePool(cmd.Context(), workspaceID, name); err != nil {
		return err
	}

	fmt.Printf("Disabled pool '%s'.\n", name)
	return nil
}

// pools enable
var poolsEnableCmd = &cobra.Command{
	Use:   "enable <name>",
	Short: "Enable a pool",
	Long:  `Enable a previously disabled pool. The pool will resume accepting new executions and launching workers.`,
	Args:  cobra.ExactArgs(1),
	RunE:  runPoolsEnable,
}

func runPoolsEnable(cmd *cobra.Command, args []string) error {
	name := args[0]

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

	if err := client.EnablePool(cmd.Context(), workspaceID, name); err != nil {
		return err
	}

	fmt.Printf("Enabled pool '%s'.\n", name)
	return nil
}

// pools export

var poolsExportOutput string
var poolsExportOnly []string

var poolsExportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export pool configuration",
	Long:  `Export all pool configurations for the workspace as TOML. Writes to stdout by default.`,
	RunE:  runPoolsExport,
}

func init() {
	poolsExportCmd.Flags().StringVarP(&poolsExportOutput, "output", "o", "", "Output file (default: stdout)")
	poolsExportCmd.Flags().StringSliceVar(&poolsExportOnly, "only", nil, "Export only named pools")
}

func runPoolsExport(cmd *cobra.Command, args []string) error {
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

	result, err := client.GetPoolConfigs(cmd.Context(), workspaceID)
	if err != nil {
		return err
	}

	pools := result.Pools

	// Filter by --only if specified
	if len(poolsExportOnly) > 0 {
		onlySet := make(map[string]bool)
		for _, name := range poolsExportOnly {
			onlySet[name] = true
		}
		for name := range pools {
			if !onlySet[name] {
				delete(pools, name)
			}
		}
	}

	tomlPools := make(map[string]any)
	for name, pool := range pools {
		tomlPools[name] = apiPoolToTOML(pool)
	}

	var buf bytes.Buffer
	encoder := toml.NewEncoder(&buf)
	encoder.Indent = ""
	if err := encoder.Encode(tomlPools); err != nil {
		return fmt.Errorf("failed to encode TOML: %w", err)
	}

	if poolsExportOutput != "" {
		if err := os.WriteFile(poolsExportOutput, buf.Bytes(), 0644); err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Exported %d pool(s) to %s\n", len(pools), poolsExportOutput)
	} else {
		fmt.Print(buf.String())
	}

	return nil
}

// pools import

var (
	poolsImportDryRun bool
	poolsImportYes    bool
	poolsImportOnly   []string
)

var poolsImportCmd = &cobra.Command{
	Use:   "import [file]",
	Short: "Import pool configuration",
	Long: `Import pool configuration from a TOML file (or stdin if no file is given).

This is a declarative operation: pools in the file are created or updated,
and pools not in the file are deleted. Use --only to restrict the scope
to specific pools.

Examples:
  coflux pools import pools.toml
  coflux pools import pools.toml --dry-run
  coflux pools import pools.toml --yes
  coflux pools import pools.toml --only gpu-workers,batch
  cat pools.toml | coflux pools import --yes`,
	Args: cobra.MaximumNArgs(1),
	RunE: runPoolsImport,
}

func init() {
	poolsImportCmd.Flags().BoolVar(&poolsImportDryRun, "dry-run", false, "Show changes without applying")
	poolsImportCmd.Flags().BoolVar(&poolsImportYes, "yes", false, "Skip confirmation prompt")
	poolsImportCmd.Flags().StringSliceVar(&poolsImportOnly, "only", nil, "Only apply changes to named pools")
}

func runPoolsImport(cmd *cobra.Command, args []string) error {
	// Determine source: positional arg or stdin
	fromStdin := len(args) == 0
	if fromStdin && !poolsImportYes && !poolsImportDryRun {
		return fmt.Errorf("--yes or --dry-run is required when reading from stdin")
	}

	// Read TOML file
	var tomlData []byte
	var err error
	if fromStdin {
		tomlData, err = io.ReadAll(os.Stdin)
	} else {
		tomlData, err = os.ReadFile(args[0])
	}
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var raw map[string]any
	if err := toml.Unmarshal(tomlData, &raw); err != nil {
		return fmt.Errorf("failed to parse TOML: %w", err)
	}
	// Support legacy format with top-level "pools" key
	if pools, ok := raw["pools"]; ok {
		if poolsMap, ok := pools.(map[string]any); ok {
			raw = poolsMap
		}
	}

	// Convert TOML pools to API format
	desiredPools := make(map[string]map[string]any)
	for name, raw := range raw {
		pool, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid pool configuration for '%s'", name)
		}
		desiredPools[name] = tomlPoolToAPI(pool)
	}

	// Connect and get current state
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

	result, err := client.GetPoolConfigs(cmd.Context(), workspaceID)
	if err != nil {
		return err
	}

	currentPools := result.Pools

	// Apply --only scoping: merge desired (for named pools) with current (for everything else)
	if len(poolsImportOnly) > 0 {
		onlySet := make(map[string]bool)
		for _, name := range poolsImportOnly {
			onlySet[name] = true
		}

		merged := make(map[string]map[string]any)
		// Keep all current pools that are out of scope
		for name, pool := range currentPools {
			if !onlySet[name] {
				merged[name] = pool
			}
		}
		// Add desired pools that are in scope
		for name, pool := range desiredPools {
			if onlySet[name] {
				merged[name] = pool
			}
		}
		desiredPools = merged
	}

	// Compute and display diff
	changes := computePoolChanges(currentPools, desiredPools)

	if len(changes) == 0 {
		fmt.Println("No changes.")
		return nil
	}

	printPoolChanges(changes, currentPools, desiredPools)

	if poolsImportDryRun {
		return nil
	}

	// Confirm
	if !poolsImportYes {
		fmt.Print("\nApply these changes? [y/N] ")
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimSpace(strings.ToLower(line))
		if line != "y" && line != "yes" {
			fmt.Println("Cancelled.")
			return nil
		}
	}

	// Apply
	if err := client.UpdatePools(cmd.Context(), workspaceID, desiredPools, result.ETag); err != nil {
		if errors.Is(err, api.ErrConflict) {
			return fmt.Errorf("pool configuration has changed since plan was generated, re-run to see updated plan")
		}
		return err
	}

	// Summarize
	var created, updated, deleted, replaced int
	for _, c := range changes {
		switch c.action {
		case "create":
			created++
		case "update":
			updated++
		case "delete":
			deleted++
		case "replace":
			replaced++
		}
	}

	var parts []string
	if created > 0 {
		parts = append(parts, fmt.Sprintf("%d created", created))
	}
	if updated > 0 {
		parts = append(parts, fmt.Sprintf("%d updated", updated))
	}
	if replaced > 0 {
		parts = append(parts, fmt.Sprintf("%d replaced", replaced))
	}
	if deleted > 0 {
		parts = append(parts, fmt.Sprintf("%d deleted", deleted))
	}
	fmt.Printf("Applied: %s.\n", strings.Join(parts, ", "))

	return nil
}

// Helper functions

func encodeProvides(provides any) string {
	if provides == nil {
		return ""
	}
	m, ok := provides.(map[string]any)
	if !ok {
		return ""
	}
	var parts []string
	for k, v := range m {
		if values, ok := v.([]any); ok {
			var vals []string
			for _, val := range values {
				if s, ok := val.(string); ok {
					vals = append(vals, s)
				}
			}
			parts = append(parts, fmt.Sprintf("%s:%s", k, strings.Join(vals, ",")))
		}
	}
	return strings.Join(parts, " ")
}

func printServerHost(launcher map[string]any) {
	serverHost := getString(launcher, "serverHost")
	serverSecure, hasSecure := launcher["serverSecure"].(bool)
	if serverHost == "" && !hasSecure {
		return
	}
	host := serverHost
	if host == "" {
		host = "(default)"
	}
	if hasSecure {
		protocol := "http"
		if serverSecure {
			protocol = "https"
		}
		fmt.Printf("Server host: %s (%s)\n", host, protocol)
	} else {
		fmt.Printf("Server host: %s\n", host)
	}
}

func parseProvides(args []string) map[string][]string {
	result := make(map[string][]string)
	for _, arg := range args {
		for _, part := range strings.Split(arg, ";") {
			if part == "" {
				continue
			}
			var key, values string
			if k, v, ok := strings.Cut(part, ":"); ok {
				key = k
				values = v
			} else {
				key = part
				values = "true"
			}
			result[key] = append(result[key], strings.Split(values, ",")...)
		}
	}
	return result
}

// TOML <-> API conversion

// inlineMap is a map that encodes as a TOML inline table.
type inlineMap struct {
	m map[string]any
}

func (im inlineMap) MarshalTOML() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	keys := make([]string, 0, len(im.m))
	for k := range im.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%s = ", k)
		if err := encodeInlineValue(&buf, im.m[k]); err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func encodeInlineValue(buf *bytes.Buffer, v any) error {
	switch val := v.(type) {
	case string:
		fmt.Fprintf(buf, "%q", val)
	case []any:
		buf.WriteByte('[')
		for i, elem := range val {
			if i > 0 {
				buf.WriteString(", ")
			}
			if err := encodeInlineValue(buf, elem); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case []string:
		buf.WriteByte('[')
		for i, elem := range val {
			if i > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(buf, "%q", elem)
		}
		buf.WriteByte(']')
	case bool:
		fmt.Fprintf(buf, "%t", val)
	case int64:
		fmt.Fprintf(buf, "%d", val)
	case float64:
		fmt.Fprintf(buf, "%v", val)
	default:
		fmt.Fprintf(buf, "%q", fmt.Sprint(val))
	}
	return nil
}

// camelCase to snake_case mapping for launcher fields
var camelToSnake = map[string]string{
	"dockerHost":            "docker_host",
	"serverHost":            "server_host",
	"serverSecure":          "server_secure",
	"serviceAccount":        "service_account",
	"apiServer":             "api_server",
	"imagePullPolicy":       "image_pull_policy",
	"imagePullSecrets":      "image_pull_secrets",
	"hostAliases":           "host_aliases",
	"nodeSelector":          "node_selector",
	"caCert":                "ca_cert",
	"activeDeadlineSeconds": "active_deadline_seconds",
	"volumeMounts":          "volume_mounts",
}

var snakeToCamel map[string]string

func init() {
	snakeToCamel = make(map[string]string, len(camelToSnake))
	for k, v := range camelToSnake {
		snakeToCamel[v] = k
	}
}

func apiPoolToTOML(pool map[string]any) map[string]any {
	result := make(map[string]any)

	if modules, ok := pool["modules"]; ok {
		result["modules"] = modules
	}
	if provides, ok := pool["provides"]; ok {
		if m, ok := provides.(map[string]any); ok {
			result["provides"] = inlineMap{m}
		} else {
			result["provides"] = provides
		}
	}
	if accepts, ok := pool["accepts"]; ok {
		if m, ok := accepts.(map[string]any); ok {
			result["accepts"] = inlineMap{m}
		} else {
			result["accepts"] = accepts
		}
	}
	if launcher, ok := pool["launcher"].(map[string]any); ok {
		result["launcher"] = apiLauncherToTOML(launcher)
	}
	return result
}

func apiLauncherToTOML(launcher map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range launcher {
		key := k
		if snakeKey, ok := camelToSnake[k]; ok {
			key = snakeKey
		}
		if key == "env" {
			if m, ok := v.(map[string]any); ok {
				result[key] = inlineMap{m}
				continue
			}
		}
		result[key] = v
	}
	return result
}

func tomlPoolToAPI(pool map[string]any) map[string]any {
	result := make(map[string]any)

	if modules, ok := pool["modules"]; ok {
		result["modules"] = toStringSlice(modules)
	}
	if provides, ok := pool["provides"]; ok {
		result["provides"] = toStringSliceMap(provides)
	}
	if accepts, ok := pool["accepts"]; ok {
		result["accepts"] = toStringSliceMap(accepts)
	}
	if launcher, ok := pool["launcher"].(map[string]any); ok {
		result["launcher"] = tomlLauncherToAPI(launcher)
	}
	return result
}

func tomlLauncherToAPI(launcher map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range launcher {
		if camelKey, ok := snakeToCamel[k]; ok {
			result[camelKey] = v
		} else {
			result[k] = v
		}
	}
	// Ensure concurrency is an integer (TOML int64 → JSON number)
	if c, ok := result["concurrency"]; ok {
		if i, ok := c.(int64); ok {
			result["concurrency"] = int(i)
		}
	}
	return result
}

// toStringSlice converts a TOML array to []string for the API
func toStringSlice(v any) []string {
	switch val := v.(type) {
	case []any:
		result := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case []string:
		return val
	default:
		return nil
	}
}

// toStringSliceMap converts TOML map values to map[string][]string
func toStringSliceMap(v any) map[string]any {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	result := make(map[string]any)
	for k, val := range m {
		result[k] = toStringSlice(val)
	}
	return result
}

// Diff computation

type poolChange struct {
	name   string
	action string // "create", "delete", "update", "replace"
}

func computePoolChanges(current, desired map[string]map[string]any) []poolChange {
	var changes []poolChange

	// Collect all pool names
	names := make(map[string]bool)
	for name := range current {
		names[name] = true
	}
	for name := range desired {
		names[name] = true
	}

	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)

	for _, name := range sorted {
		cur, inCurrent := current[name]
		des, inDesired := desired[name]

		if !inCurrent && inDesired {
			changes = append(changes, poolChange{name: name, action: "create"})
		} else if inCurrent && !inDesired {
			changes = append(changes, poolChange{name: name, action: "delete"})
		} else if inCurrent && inDesired {
			if poolsEqual(cur, des) {
				continue
			}
			// Check if launcher type changed → replace
			curType := getLauncherType(cur)
			desType := getLauncherType(des)
			if curType != "" && desType != "" && curType != desType {
				changes = append(changes, poolChange{name: name, action: "replace"})
			} else {
				changes = append(changes, poolChange{name: name, action: "update"})
			}
		}
	}

	return changes
}

func getLauncherType(pool map[string]any) string {
	if launcher, ok := pool["launcher"].(map[string]any); ok {
		if t, ok := launcher["type"].(string); ok {
			return t
		}
	}
	return ""
}

func poolsEqual(a, b map[string]any) bool {
	aj, _ := json.Marshal(normalizeForComparison(a))
	bj, _ := json.Marshal(normalizeForComparison(b))
	return string(aj) == string(bj)
}

// normalizeForComparison normalizes a pool config for comparison.
// Removes empty/nil values and sorts maps for consistent JSON output.
func normalizeForComparison(v any) any {
	switch val := v.(type) {
	case map[string]any:
		result := make(map[string]any)
		for k, v := range val {
			nv := normalizeForComparison(v)
			if !isEmptyValue(nv) {
				result[k] = nv
			}
		}
		if len(result) == 0 {
			return nil
		}
		return result
	case []any:
		result := make([]any, len(val))
		for i, item := range val {
			result[i] = normalizeForComparison(item)
		}
		return result
	case []string:
		// Convert to []any for consistent handling
		result := make([]any, len(val))
		for i, s := range val {
			result[i] = s
		}
		return result
	case int64:
		return float64(val)
	case int:
		return float64(val)
	default:
		return v
	}
}

func isEmptyValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map, reflect.Slice:
		return rv.Len() == 0
	case reflect.String:
		return rv.String() == ""
	}
	return false
}

// Diff display

func printPoolChanges(changes []poolChange, current, desired map[string]map[string]any) {
	for _, c := range changes {
		switch c.action {
		case "create":
			fmt.Printf("\n  %s+ %s%s (create)\n", colorGreen, c.name, colorReset)
			printPoolConfig(desired[c.name], "    ", colorGreen)

		case "delete":
			fmt.Printf("\n  %s- %s%s (delete)\n", colorRed, c.name, colorReset)
			printPoolConfig(current[c.name], "    ", colorRed)

		case "replace":
			fmt.Printf("\n  %s~ %s%s (replace — launcher type changed)\n", colorYellow, c.name, colorReset)
			fmt.Printf("    %sBefore:%s\n", colorRed, colorReset)
			printPoolConfig(current[c.name], "      ", colorRed)
			fmt.Printf("    %sAfter:%s\n", colorGreen, colorReset)
			printPoolConfig(desired[c.name], "      ", colorGreen)

		case "update":
			fmt.Printf("\n  %s~ %s%s (update)\n", colorYellow, c.name, colorReset)
			printFieldDiffs(current[c.name], desired[c.name], "    ")
		}
	}
	fmt.Println()
}

func printPoolConfig(pool map[string]any, indent, color string) {
	if modules := getAnySlice(pool, "modules"); len(modules) > 0 {
		fmt.Printf("%s%smodules: %s%s\n", indent, color, formatSlice(modules), colorReset)
	}
	if provides, ok := pool["provides"].(map[string]any); ok && len(provides) > 0 {
		fmt.Printf("%s%sprovides: %s%s\n", indent, color, formatTagSet(provides), colorReset)
	}
	if accepts, ok := pool["accepts"].(map[string]any); ok && len(accepts) > 0 {
		fmt.Printf("%s%saccepts: %s%s\n", indent, color, formatTagSet(accepts), colorReset)
	}
	if launcher, ok := pool["launcher"].(map[string]any); ok {
		for _, key := range sortedKeys(launcher) {
			fmt.Printf("%s%slauncher.%s: %s%s\n", indent, color, key, formatValue(launcher[key]), colorReset)
		}
	}
}

func printFieldDiffs(before, after map[string]any, indent string) {
	// Collect all top-level field paths and compare
	type fieldDiff struct {
		path      string
		before    any
		after     any
		isChanged bool
	}

	var diffs []fieldDiff

	// Compare simple fields
	for _, field := range []string{"modules", "provides", "accepts"} {
		bv := before[field]
		av := after[field]
		bj, _ := json.Marshal(normalizeForComparison(bv))
		aj, _ := json.Marshal(normalizeForComparison(av))
		if string(bj) != string(aj) {
			diffs = append(diffs, fieldDiff{path: field, before: bv, after: av, isChanged: true})
		}
	}

	// Compare launcher fields individually
	bLauncher, _ := before["launcher"].(map[string]any)
	aLauncher, _ := after["launcher"].(map[string]any)
	if bLauncher != nil || aLauncher != nil {
		if bLauncher == nil {
			bLauncher = make(map[string]any)
		}
		if aLauncher == nil {
			aLauncher = make(map[string]any)
		}

		// For env, node_selector — diff per key
		allLauncherKeys := make(map[string]bool)
		for k := range bLauncher {
			allLauncherKeys[k] = true
		}
		for k := range aLauncher {
			allLauncherKeys[k] = true
		}

		sortedLKeys := make([]string, 0, len(allLauncherKeys))
		for k := range allLauncherKeys {
			sortedLKeys = append(sortedLKeys, k)
		}
		sort.Strings(sortedLKeys)

		for _, k := range sortedLKeys {
			bv := bLauncher[k]
			av := aLauncher[k]

			// For map fields (env, nodeSelector, etc.), diff per key
			bMap, bIsMap := bv.(map[string]any)
			aMap, aIsMap := av.(map[string]any)
			if bIsMap || aIsMap {
				if bMap == nil {
					bMap = make(map[string]any)
				}
				if aMap == nil {
					aMap = make(map[string]any)
				}
				allSubKeys := make(map[string]bool)
				for sk := range bMap {
					allSubKeys[sk] = true
				}
				for sk := range aMap {
					allSubKeys[sk] = true
				}
				sortedSubKeys := make([]string, 0, len(allSubKeys))
				for sk := range allSubKeys {
					sortedSubKeys = append(sortedSubKeys, sk)
				}
				sort.Strings(sortedSubKeys)

				for _, sk := range sortedSubKeys {
					sbv := bMap[sk]
					sav := aMap[sk]
					if !valuesEqual(sbv, sav) {
						diffs = append(diffs, fieldDiff{
							path:      "launcher." + k + "." + sk,
							before:    sbv,
							after:     sav,
							isChanged: true,
						})
					}
				}
			} else if !valuesEqual(bv, av) {
				diffs = append(diffs, fieldDiff{
					path:      "launcher." + k,
					before:    bv,
					after:     av,
					isChanged: true,
				})
			}
		}
	}

	for _, d := range diffs {
		if d.before == nil {
			fmt.Printf("%s%s+ %s: %s%s\n", indent, colorGreen, d.path, formatValue(d.after), colorReset)
		} else if d.after == nil {
			fmt.Printf("%s%s- %s: %s%s\n", indent, colorRed, d.path, formatValue(d.before), colorReset)
		} else {
			fmt.Printf("%s%s~ %s: %s -> %s%s\n", indent, colorYellow, d.path, formatValue(d.before), formatValue(d.after), colorReset)
		}
	}
}

func valuesEqual(a, b any) bool {
	aj, _ := json.Marshal(normalizeForComparison(a))
	bj, _ := json.Marshal(normalizeForComparison(b))
	return string(aj) == string(bj)
}

func formatValue(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case bool:
		return fmt.Sprintf("%v", val)
	case float64:
		if val == float64(int(val)) {
			return fmt.Sprintf("%d", int(val))
		}
		return fmt.Sprintf("%g", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case int:
		return fmt.Sprintf("%d", val)
	case []any:
		return formatSlice(val)
	case []string:
		items := make([]any, len(val))
		for i, s := range val {
			items[i] = s
		}
		return formatSlice(items)
	case map[string]any:
		return formatMap(val)
	case nil:
		return "(none)"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func formatSlice(items []any) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, fmt.Sprintf("%v", item))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func formatMap(m map[string]any) string {
	keys := sortedKeys(m)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, m[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func formatTagSet(tags map[string]any) string {
	keys := sortedKeys(tags)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		if vals, ok := tags[k].([]any); ok {
			var vs []string
			for _, v := range vals {
				vs = append(vs, fmt.Sprintf("%v", v))
			}
			parts = append(parts, k+":"+strings.Join(vs, ","))
		}
	}
	return strings.Join(parts, " ")
}

func getAnySlice(m map[string]any, key string) []any {
	if v, ok := m[key].([]any); ok {
		return v
	}
	return nil
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
