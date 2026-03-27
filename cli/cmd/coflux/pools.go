package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var poolsCmd = &cobra.Command{
	Use:   "pools",
	Short: "Manage pools",
}

func init() {
	poolsCmd.AddCommand(poolsListCmd)
	poolsCmd.AddCommand(poolsGetCmd)
	poolsCmd.AddCommand(poolsUpdateCmd)
	poolsCmd.AddCommand(poolsDeleteCmd)
	poolsCmd.AddCommand(poolsLaunchesCmd)
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
		rows = append(rows, []string{name, launcher, modules, provides})
	}

	printTable([]string{"Name", "Launcher", "Modules", "Provides"}, rows)
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

	// Launcher
	if launcher, ok := pool["launcher"].(map[string]any); ok {
		fmt.Printf("Launcher: %s\n", getString(launcher, "type"))
		if image := getString(launcher, "image"); image != "" {
			fmt.Printf("Image: %s\n", image)
		}
		if cli := getString(launcher, "cli"); cli != "" {
			fmt.Printf("CLI: %s\n", cli)
		}
		if cwd := getString(launcher, "cwd"); cwd != "" {
			fmt.Printf("Working directory: %s\n", cwd)
		}
		if dockerHost := getString(launcher, "dockerHost"); dockerHost != "" {
			fmt.Printf("Docker host: %s\n", dockerHost)
		}
		if serverHost := getString(launcher, "serverHost"); serverHost != "" {
			fmt.Printf("Server host: %s\n", serverHost)
		}
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

	return watchTopics(cmd.Context(), getHost(), isSecure(), token,
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

func hasCommonLauncherFlags() bool {
	return poolsUpdateServerHost != "" || poolsUpdateNoServerHost ||
		poolsUpdateAdapter != nil || poolsUpdateNoAdapter ||
		poolsUpdateConcurrency > 0 || poolsUpdateNoConcurrency ||
		poolsUpdateEnv != nil || poolsUpdateNoEnv
}

func applyCommonLauncherFlags(launcher map[string]any) {
	if poolsUpdateServerHost != "" {
		launcher["serverHost"] = poolsUpdateServerHost
	} else if poolsUpdateNoServerHost {
		delete(launcher, "serverHost")
	}
	if poolsUpdateAdapter != nil {
		launcher["adapter"] = poolsUpdateAdapter
	} else if poolsUpdateNoAdapter {
		delete(launcher, "adapter")
	}
	if poolsUpdateConcurrency > 0 {
		launcher["concurrency"] = poolsUpdateConcurrency
	} else if poolsUpdateNoConcurrency {
		delete(launcher, "concurrency")
	}
	if poolsUpdateEnv != nil {
		env := make(map[string]any)
		for _, e := range poolsUpdateEnv {
			if key, value, ok := strings.Cut(e, "="); ok {
				env[key] = value
			}
		}
		launcher["env"] = env
	} else if poolsUpdateNoEnv {
		delete(launcher, "env")
	}
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

// pools update
var (
	poolsUpdateModules       []string
	poolsUpdateProvides      []string
	poolsUpdateDockerImage   string
	poolsUpdateDockerHost    string
	poolsUpdateNoDockerHost  bool
	poolsUpdateProcessCli    string
	poolsUpdateProcessCwd    string
	poolsUpdateNoProcessCwd  bool
	poolsUpdateServerHost    string
	poolsUpdateNoServerHost  bool
	poolsUpdateAdapter       []string
	poolsUpdateNoAdapter     bool
	poolsUpdateConcurrency   int
	poolsUpdateNoConcurrency bool
	poolsUpdateEnv           []string
	poolsUpdateNoEnv         bool
)

var poolsUpdateCmd = &cobra.Command{
	Use:   "update <name>",
	Short: "Update a pool",
	Args:  cobra.ExactArgs(1),
	RunE:  runPoolsUpdate,
}

func init() {
	poolsUpdateCmd.Flags().StringSliceVarP(&poolsUpdateModules, "module", "m", nil, "Modules to be hosted")
	poolsUpdateCmd.Flags().StringSliceVar(&poolsUpdateProvides, "provides", nil, "Features that workers provide")
	poolsUpdateCmd.Flags().StringVar(&poolsUpdateDockerImage, "docker-image", "", "Docker image")
	poolsUpdateCmd.Flags().StringVar(&poolsUpdateDockerHost, "docker-host", "", "Docker host")
	poolsUpdateCmd.Flags().BoolVar(&poolsUpdateNoDockerHost, "no-docker-host", false, "Unset Docker host (use default socket)")
	poolsUpdateCmd.Flags().StringVar(&poolsUpdateProcessCli, "process-cli", "", "Path to coflux CLI binary (process launcher)")
	poolsUpdateCmd.Flags().StringVar(&poolsUpdateProcessCwd, "process-cwd", "", "Working directory for process launcher")
	poolsUpdateCmd.Flags().BoolVar(&poolsUpdateNoProcessCwd, "no-process-cwd", false, "Unset working directory")
	poolsUpdateCmd.Flags().StringVar(&poolsUpdateServerHost, "server-host", "", "Coflux server host (overrides server default)")
	poolsUpdateCmd.Flags().BoolVar(&poolsUpdateNoServerHost, "no-server-host", false, "Unset server host (use server default)")
	poolsUpdateCmd.Flags().StringSliceVar(&poolsUpdateAdapter, "adapter", nil, "Adapter command (e.g., --adapter python,-m,coflux)")
	poolsUpdateCmd.Flags().BoolVar(&poolsUpdateNoAdapter, "no-adapter", false, "Unset adapter (use worker default)")
	poolsUpdateCmd.Flags().IntVar(&poolsUpdateConcurrency, "concurrency", 0, "Max concurrent executions per worker")
	poolsUpdateCmd.Flags().BoolVar(&poolsUpdateNoConcurrency, "no-concurrency", false, "Unset concurrency (use worker default)")
	poolsUpdateCmd.Flags().StringArrayVar(&poolsUpdateEnv, "env", nil, "Environment variable (e.g., --env KEY=VALUE)")
	poolsUpdateCmd.Flags().BoolVar(&poolsUpdateNoEnv, "no-env", false, "Clear all custom environment variables")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("docker-host", "no-docker-host")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("server-host", "no-server-host")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("process-cwd", "no-process-cwd")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("adapter", "no-adapter")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("concurrency", "no-concurrency")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("env", "no-env")
	// Process and Docker flags are mutually exclusive
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("docker-image", "process-cli")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("docker-host", "process-cli")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("no-docker-host", "process-cli")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("docker-image", "process-cwd")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("docker-host", "process-cwd")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("no-docker-host", "process-cwd")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("docker-image", "no-process-cwd")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("docker-host", "no-process-cwd")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("no-docker-host", "no-process-cwd")
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

	// Get existing pool
	pool, _ := client.GetPool(cmd.Context(), workspaceID, name)
	if pool == nil {
		pool = make(map[string]any)
	}

	// Apply updates
	if poolsUpdateModules != nil {
		pool["modules"] = poolsUpdateModules
	}
	if poolsUpdateProvides != nil {
		pool["provides"] = parseProvides(poolsUpdateProvides)
	}
	if poolsUpdateProcessCli != "" || poolsUpdateProcessCwd != "" || poolsUpdateNoProcessCwd {
		launcher, ok := pool["launcher"].(map[string]any)
		if !ok || getString(launcher, "type") != "process" {
			launcher = map[string]any{"type": "process"}
		}
		if poolsUpdateProcessCli != "" {
			launcher["cli"] = poolsUpdateProcessCli
		}
		if poolsUpdateProcessCwd != "" {
			launcher["cwd"] = poolsUpdateProcessCwd
		} else if poolsUpdateNoProcessCwd {
			delete(launcher, "cwd")
		}
		applyCommonLauncherFlags(launcher)
		pool["launcher"] = launcher
	} else if poolsUpdateDockerImage != "" || poolsUpdateDockerHost != "" || poolsUpdateNoDockerHost {
		launcher, ok := pool["launcher"].(map[string]any)
		if !ok || getString(launcher, "type") != "docker" {
			launcher = map[string]any{"type": "docker"}
		}
		if poolsUpdateDockerImage != "" {
			launcher["image"] = poolsUpdateDockerImage
		}
		if poolsUpdateDockerHost != "" {
			launcher["dockerHost"] = poolsUpdateDockerHost
		} else if poolsUpdateNoDockerHost {
			delete(launcher, "dockerHost")
		}
		applyCommonLauncherFlags(launcher)
		pool["launcher"] = launcher
	} else if hasCommonLauncherFlags() {
		// Update common launcher fields on an existing launcher
		if launcher, ok := pool["launcher"].(map[string]any); ok {
			applyCommonLauncherFlags(launcher)
			pool["launcher"] = launcher
		}
	}

	if err := client.UpdatePool(cmd.Context(), workspaceID, name, pool); err != nil {
		return err
	}

	fmt.Printf("Updated pool '%s'.\n", name)
	return nil
}

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
