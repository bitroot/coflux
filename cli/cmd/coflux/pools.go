package main

import (
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	topicalclient "github.com/bitroot/coflux/cli/internal/topical"
	"github.com/spf13/cobra"
	"golang.org/x/term"
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
		return fmt.Errorf("pool '%s' not found", name)
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
		if dockerHost := getString(launcher, "dockerHost"); dockerHost != "" {
			fmt.Printf("Docker host: %s\n", dockerHost)
		}
		if serverHost := getString(launcher, "serverHost"); serverHost != "" {
			fmt.Printf("Server host: %s\n", serverHost)
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

	data, err := client.CaptureTopic(cmd.Context(), "pools/"+workspaceID+"/"+poolName)
	if err != nil {
		return fmt.Errorf("pool '%s' not found", poolName)
	}

	workers, _ := data["workers"].(map[string]any)

	if isOutput("json") {
		return outputJSON(workers)
	}

	renderWorkerTable(data, 0, 0)
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

	topicPath := "pools/" + workspaceID + "/" + poolName

	client, err := topicalclient.Connect(cmd.Context(), getHost(), isSecure(), token)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	sub := client.Subscribe(topicPath, nil)
	defer sub.Unsubscribe()

	sigCtx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	linesDrawn := 0
	maxLines := 0
	if _, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil && h > 0 {
		maxLines = h - 1
	}

	for {
		select {
		case value, ok := <-sub.Values():
			if !ok {
				return fmt.Errorf("subscription closed unexpectedly")
			}

			data, ok := value.(map[string]any)
			if !ok {
				continue
			}

			linesDrawn = renderWorkerTable(data, linesDrawn, maxLines)

		case err, ok := <-sub.Err():
			if !ok {
				return fmt.Errorf("subscription closed unexpectedly")
			}
			return fmt.Errorf("subscription error: %w", err)

		case <-sigCtx.Done():
			return nil
		}
	}
}

type workerRow struct {
	id     string
	status string
	detail string
	ts     int64
}

func buildWorkerRows(workers map[string]any) []workerRow {
	var rows []workerRow
	for id, raw := range workers {
		w, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		status, detail, ts := workerStatus(w)
		rows = append(rows, workerRow{id: id, status: status, detail: detail, ts: ts})
	}
	// Sort by timestamp descending (most recent first)
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].ts > rows[j].ts
	})
	return rows
}

func renderWorkerTable(data map[string]any, linesDrawn int, maxLines int) int {
	workers, _ := data["workers"].(map[string]any)
	if workers == nil {
		return linesDrawn
	}

	rows := buildWorkerRows(workers)

	// Move cursor up to overwrite previous output
	if linesDrawn > 0 {
		fmt.Printf("\033[%dA", linesDrawn)
	}

	if len(rows) == 0 {
		fmt.Printf("\r%sNo launches.%s\033[K\n", colorDim, colorReset)
		for i := 1; i < linesDrawn; i++ {
			fmt.Print("\r\033[K\n")
		}
		return max(1, linesDrawn)
	}

	// Truncate if needed
	displayRows := rows
	truncated := 0
	if maxLines > 0 && len(displayRows) > maxLines {
		truncated = len(displayRows) - (maxLines - 1)
		displayRows = displayRows[:maxLines-1]
	}

	outputLines := 0
	for _, r := range displayRows {
		indicator := workerStatusIndicator(r.status)
		ts := colorDim + formatMillis(r.ts) + colorReset
		line := fmt.Sprintf("%s  %s  %s", indicator, ts, r.id)
		if r.detail != "" {
			line += "  " + r.detail
		}
		fmt.Printf("\r%s\033[K\n", line)
		outputLines++
	}
	if truncated > 0 {
		fmt.Printf("\r%s... %d more (%d total)%s\033[K\n", colorDim, truncated, len(rows), colorReset)
		outputLines++
	}

	// Clear leftover lines
	for i := outputLines; i < linesDrawn; i++ {
		fmt.Print("\r\033[K\n")
	}

	return max(outputLines, linesDrawn)
}

func workerStatus(w map[string]any) (status, detail string, ts int64) {
	startingAt := getFloat64(w, "startingAt")
	startedAt := getFloat64(w, "startedAt")
	deactivatedAt := getFloat64(w, "deactivatedAt")
	stoppingAt := getFloat64(w, "stoppingAt")
	startError, _ := w["startError"].(string)
	workerError, _ := w["error"].(string)
	connected, _ := w["connected"].(bool)

	if deactivatedAt > 0 {
		ts = int64(deactivatedAt)
		if workerError != "" || startError != "" {
			errMsg := workerError
			if errMsg == "" {
				errMsg = startError
			}
			return "failed", colorRed + errMsg + colorReset, ts
		}
		return "stopped", "", ts
	}
	if stoppingAt > 0 {
		ts = int64(stoppingAt)
		return "stopping", colorDim + "Stopping..." + colorReset, ts
	}
	if startedAt > 0 {
		ts = int64(startedAt)
		if connected {
			return "running", colorDim + "Connected" + colorReset, ts
		}
		return "started", colorDim + "Started" + colorReset, ts
	}
	if startError != "" {
		ts = int64(startingAt)
		return "failed", colorRed + startError + colorReset, ts
	}
	if startingAt > 0 {
		ts = int64(startingAt)
		return "starting", colorDim + "Starting..." + colorReset, ts
	}
	return "unknown", "", 0
}

func workerStatusIndicator(status string) string {
	switch status {
	case "running":
		return colorGreen + "●" + colorReset
	case "started":
		return colorBlue + "●" + colorReset
	case "starting":
		return colorDim + "●" + colorReset
	case "stopping":
		return colorYellow + "●" + colorReset
	case "stopped":
		return colorDim + "●" + colorReset
	case "failed":
		return colorRed + "●" + colorReset
	default:
		return colorDim + "●" + colorReset
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

	data, err := client.CaptureTopic(cmd.Context(), "pools/"+workspaceID+"/"+poolName)
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
	status, _, _ := workerStatus(w)
	fmt.Printf("Status: %s\n", status)

	if v := getFloat64(w, "startingAt"); v > 0 {
		fmt.Printf("Starting at: %s\n", formatMillis(int64(v)))
	}
	if v := getFloat64(w, "startedAt"); v > 0 {
		fmt.Printf("Started at: %s\n", formatMillis(int64(v)))
	}
	if s, _ := w["startError"].(string); s != "" {
		fmt.Printf("Start error: %s\n", s)
	}
	if v := getFloat64(w, "stoppingAt"); v > 0 {
		fmt.Printf("Stopping at: %s\n", formatMillis(int64(v)))
	}
	if v := getFloat64(w, "stoppedAt"); v > 0 {
		fmt.Printf("Stopped at: %s\n", formatMillis(int64(v)))
	}
	if s, _ := w["stopError"].(string); s != "" {
		fmt.Printf("Stop error: %s\n", s)
	}
	if v := getFloat64(w, "deactivatedAt"); v > 0 {
		fmt.Printf("Deactivated at: %s\n", formatMillis(int64(v)))
	}
	if s, _ := w["error"].(string); s != "" {
		fmt.Printf("Error: %s\n", s)
	}
	if connected, ok := w["connected"].(bool); ok {
		fmt.Printf("Connected: %v\n", connected)
	}

	if logs, _ := w["logs"].(string); logs != "" {
		fmt.Printf("\nLogs:\n%s\n", logs)
	}

	return nil
}

func formatMillis(ms int64) string {
	t := time.Unix(ms/1000, (ms%1000)*int64(time.Millisecond))
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

// pools update
var (
	poolsUpdateModules      []string
	poolsUpdateProvides     []string
	poolsUpdateDockerImage  string
	poolsUpdateDockerHost   string
	poolsUpdateNoDockerHost bool
	poolsUpdateServerHost   string
	poolsUpdateNoServerHost bool
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
	poolsUpdateCmd.Flags().StringVar(&poolsUpdateServerHost, "server-host", "", "Coflux server host (overrides server default)")
	poolsUpdateCmd.Flags().BoolVar(&poolsUpdateNoServerHost, "no-server-host", false, "Unset server host (use server default)")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("docker-host", "no-docker-host")
	poolsUpdateCmd.MarkFlagsMutuallyExclusive("server-host", "no-server-host")
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

	workspaceID, err := resolveWorkspaceID(cmd.Context(), client, workspace)
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
	if poolsUpdateDockerImage != "" || poolsUpdateDockerHost != "" || poolsUpdateNoDockerHost || poolsUpdateServerHost != "" || poolsUpdateNoServerHost {
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
		if poolsUpdateServerHost != "" {
			launcher["serverHost"] = poolsUpdateServerHost
		} else if poolsUpdateNoServerHost {
			delete(launcher, "serverHost")
		}
		pool["launcher"] = launcher
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
