package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"

	topicalclient "github.com/bitroot/coflux/cli/internal/topical"
	"github.com/spf13/cobra"
)

var queueNoWatch bool

var queueCmd = &cobra.Command{
	Use:   "queue",
	Short: "Show the execution queue",
	RunE:  runQueue,
}

func init() {
	queueCmd.Flags().BoolVar(&queueNoWatch, "no-watch", false, "Show queue snapshot and exit")
}

type queueEntry struct {
	id           string
	module       string
	target       string
	executeAfter float64
	assignedAt   float64
	dependencies []string
	requires     map[string][]string
}

func parseQueueEntries(data map[string]any) []queueEntry {
	var entries []queueEntry
	for id, raw := range data {
		e, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		entry := queueEntry{id: id}
		entry.module, _ = e["module"].(string)
		entry.target, _ = e["target"].(string)
		if ea, ok := e["executeAfter"].(float64); ok {
			entry.executeAfter = ea
		}
		if aa, ok := e["assignedAt"].(float64); ok {
			entry.assignedAt = aa
		}
		if deps, ok := e["dependencies"].([]any); ok {
			for _, d := range deps {
				if s, ok := d.(string); ok {
					entry.dependencies = append(entry.dependencies, s)
				}
			}
		}
		if req, ok := e["requires"].(map[string]any); ok {
			entry.requires = make(map[string][]string)
			for k, v := range req {
				if arr, ok := v.([]any); ok {
					for _, val := range arr {
						if sv, ok := val.(string); ok {
							entry.requires[k] = append(entry.requires[k], sv)
						}
					}
				}
			}
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].id < entries[j].id
	})
	return entries
}

type queueStatus string

const (
	queueStatusScheduled    queueStatus = "scheduled"
	queueStatusPaused       queueStatus = "paused"
	queueStatusDependencies queueStatus = "dependencies"
	queueStatusNoSession    queueStatus = "no-session"
	queueStatusUnknown      queueStatus = "unknown"
	queueStatusAssigned     queueStatus = "assigned"
)

func getQueueEntryStatus(e queueEntry, sessions []sessionEntry, workspaceState string) queueStatus {
	if e.assignedAt > 0 {
		return queueStatusAssigned
	}
	if e.executeAfter > 0 && e.executeAfter > float64(time.Now().UnixMilli()) {
		return queueStatusScheduled
	}
	if workspaceState == "paused" {
		return queueStatusPaused
	}
	if len(e.dependencies) > 0 {
		return queueStatusDependencies
	}
	if !hasCompatibleSessionForEntry(e, sessions) {
		return queueStatusNoSession
	}
	return queueStatusUnknown
}

func hasCompatibleSessionForEntry(e queueEntry, sessions []sessionEntry) bool {
	for _, s := range sessions {
		if !s.connected {
			continue
		}
		if s.workerState != "" && s.workerState != "active" {
			continue
		}
		if s.concurrency > 0 && s.executions >= s.concurrency {
			continue
		}
		if !sessionHasTarget(s, e.module, e.target) {
			continue
		}
		if !sessionSatisfiesRequires(s, e.requires) {
			continue
		}
		return true
	}
	return false
}

func sessionHasTarget(s sessionEntry, module, target string) bool {
	if s.targetMap == nil {
		return false
	}
	targets, ok := s.targetMap[module]
	if !ok {
		return false
	}
	for _, t := range targets {
		if t == target {
			return true
		}
	}
	return false
}

func sessionSatisfiesRequires(s sessionEntry, requires map[string][]string) bool {
	for k, values := range requires {
		provided, ok := s.provides[k]
		if !ok {
			return false
		}
		found := false
		for _, v := range values {
			for _, p := range provided {
				if v == p {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func queueStatusIndicator(status queueStatus) string {
	switch status {
	case queueStatusAssigned:
		return colorBlue + "●" + colorReset
	case queueStatusScheduled:
		return colorDim + "●" + colorReset
	case queueStatusPaused:
		return colorYellow + "●" + colorReset
	case queueStatusDependencies:
		return "\033[38;5;208m●" + colorReset // orange
	case queueStatusNoSession:
		return colorRed + "●" + colorReset
	case queueStatusUnknown:
		return "\033[38;5;217m●" + colorReset // light red
	default:
		return colorDim + "●" + colorReset
	}
}

func queueStatusLabel(status queueStatus) string {
	switch status {
	case queueStatusAssigned:
		return "assigned"
	case queueStatusScheduled:
		return "scheduled"
	case queueStatusPaused:
		return "paused"
	case queueStatusDependencies:
		return "waiting"
	case queueStatusNoSession:
		return "no session"
	case queueStatusUnknown:
		return "unknown"
	default:
		return ""
	}
}

func runQueue(cmd *cobra.Command, args []string) error {
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
		data, err := client.CaptureTopic(cmd.Context(), "workspaces/"+workspaceID+"/queue")
		if err != nil {
			return err
		}
		return outputJSON(data)
	}

	if queueNoWatch {
		data, err := client.CaptureTopic(cmd.Context(), "workspaces/"+workspaceID+"/queue")
		if err != nil {
			return err
		}
		sessionsData, err := client.CaptureTopic(cmd.Context(), "workspaces/"+workspaceID+"/sessions")
		if err != nil {
			return err
		}
		entries := parseQueueEntries(data)
		sessions := parseSessionEntriesWithTargets(sessionsData)
		printQueueTable(entries, sessions, "")
		return nil
	}

	return watchQueue(cmd.Context(), getHost(), isSecure(), token, workspaceID, workspace)
}

func printQueueTable(entries []queueEntry, sessions []sessionEntry, workspaceState string) {
	if len(entries) == 0 {
		fmt.Println("Queue is empty.")
		return
	}

	lines := renderQueueTable(entries, sessions, workspaceState)
	for _, line := range lines {
		fmt.Println(line)
	}
}

func watchQueue(ctx context.Context, host string, secure bool, token string, workspaceID string, workspaceName string) error {
	client, err := topicalclient.Connect(ctx, host, secure, token)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	queueSub := client.Subscribe("workspaces/"+workspaceID+"/queue", nil)
	defer queueSub.Unsubscribe()

	sessionsSub := client.Subscribe("workspaces/"+workspaceID+"/sessions", nil)
	defer sessionsSub.Unsubscribe()

	// We also need workspace state to check if paused
	workspacesSub := client.Subscribe("workspaces", nil)
	defer workspacesSub.Unsubscribe()

	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var queueData map[string]any
	var sessionsData map[string]any
	var workspacesData map[string]any
	linesDrawn := 0

	// Get terminal height for capping live output
	maxLines := 0
	if _, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil && h > 0 {
		maxLines = h - 1 // leave room for the cursor line
	}

	render := func() {
		if queueData == nil || sessionsData == nil {
			return
		}

		entries := parseQueueEntries(queueData)
		sessions := parseSessionEntriesWithTargets(sessionsData)

		workspaceState := ""
		if workspacesData != nil {
			for _, ws := range workspacesData {
				if wsMap, ok := ws.(map[string]any); ok {
					if name, _ := wsMap["name"].(string); name == workspaceName {
						workspaceState, _ = wsMap["state"].(string)
						break
					}
				}
			}
		}

		// Move cursor up to overwrite previous output
		if linesDrawn > 0 {
			fmt.Printf("\033[%dA", linesDrawn)
		}

		lines := renderQueueTable(entries, sessions, workspaceState)
		totalRows := len(lines) - 1 // exclude header
		truncated := 0
		if maxLines > 0 && len(lines) > maxLines {
			truncated = len(lines) - (maxLines - 1)
			lines = lines[:maxLines-1]
		}
		for _, line := range lines {
			fmt.Printf("\r%s\033[K\n", line)
		}
		if truncated > 0 {
			fmt.Printf("\r%s… %d more executions (%d total)%s\033[K\n", colorDim, truncated, totalRows, colorReset)
		}
		outputLines := len(lines)
		if truncated > 0 {
			outputLines++
		}
		fmt.Print("\033[J") // clear from cursor to end of screen
		linesDrawn = outputLines
	}

	for {
		select {
		case value, ok := <-queueSub.Values():
			if !ok {
				return fmt.Errorf("subscription closed unexpectedly")
			}
			if data, ok := value.(map[string]any); ok {
				queueData = data
				render()
			}

		case value, ok := <-sessionsSub.Values():
			if !ok {
				return fmt.Errorf("subscription closed unexpectedly")
			}
			if data, ok := value.(map[string]any); ok {
				sessionsData = data
				render()
			}

		case value, ok := <-workspacesSub.Values():
			if !ok {
				return fmt.Errorf("subscription closed unexpectedly")
			}
			if data, ok := value.(map[string]any); ok {
				workspacesData = data
				render()
			}

		case err, ok := <-queueSub.Err():
			if !ok {
				return fmt.Errorf("subscription closed unexpectedly")
			}
			return fmt.Errorf("subscription error: %w", err)

		case err, ok := <-sessionsSub.Err():
			if !ok {
				return fmt.Errorf("subscription closed unexpectedly")
			}
			return fmt.Errorf("subscription error: %w", err)

		case err, ok := <-workspacesSub.Err():
			if !ok {
				return fmt.Errorf("subscription closed unexpectedly")
			}
			return fmt.Errorf("subscription error: %w", err)

		case <-sigCtx.Done():
			return nil
		}
	}
}

// parseSessionEntriesWithTargets parses session data including raw target maps
// for compatibility checking.
func parseSessionEntriesWithTargets(data map[string]any) []sessionEntry {
	entries := parseSessionEntries(data)
	// Re-parse to populate targetMap for compatibility checking
	for i, entry := range entries {
		if raw, ok := data[entry.id]; ok {
			if s, ok := raw.(map[string]any); ok {
				if targets, ok := s["targets"].(map[string]any); ok {
					entries[i].targetMap = make(map[string][]string)
					for module, names := range targets {
						if arr, ok := names.([]any); ok {
							for _, n := range arr {
								if name, ok := n.(string); ok {
									entries[i].targetMap[module] = append(entries[i].targetMap[module], name)
								}
							}
						}
					}
				}
			}
		}
	}
	return entries
}

func renderQueueTable(entries []queueEntry, sessions []sessionEntry, workspaceState string) []string {
	headers := []string{"Execution", "Target", "Requires", "Dependencies", "Status"}
	var rows [][]string
	for _, e := range entries {
		status := getQueueEntryStatus(e, sessions, workspaceState)
		target := e.target + " " + colorDim + "(" + e.module + ")" + colorReset
		deps := ""
		if len(e.dependencies) > 0 {
			deps = strings.Join(e.dependencies, ", ")
		}
		rows = append(rows, []string{
			queueStatusIndicator(status) + " " + e.id,
			target,
			formatProvides(e.requires),
			deps,
			colorDim + queueStatusLabel(status) + colorReset,
		})
	}
	return formatTable(headers, rows)
}
