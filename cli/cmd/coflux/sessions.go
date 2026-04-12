package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/spf13/cobra"
)

var sessionsCmd = &cobra.Command{
	Use:   "sessions",
	Short: "Manage sessions",
}

var sessionsListWatch bool

var sessionsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List active sessions",
	RunE:  runSessionsList,
}

func init() {
	sessionsCmd.AddCommand(sessionsListCmd)
	sessionsListCmd.Flags().BoolVar(&sessionsListWatch, "watch", false, "Watch for changes")
}

type sessionEntry struct {
	id          string
	connected   bool
	poolName    string
	workerState string
	executions  int
	concurrency int
	targets     int
	provides    map[string][]string
	targetMap   map[string][]string // module -> []target, populated when needed for matching
}

func parseSessionEntries(data map[string]any) []sessionEntry {
	var entries []sessionEntry
	for id, raw := range data {
		s, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		entry := sessionEntry{id: id}
		entry.connected, _ = s["connected"].(bool)
		if pn, ok := s["poolName"].(string); ok {
			entry.poolName = pn
		}
		if ws, ok := s["workerState"].(string); ok {
			entry.workerState = ws
		}
		if e, ok := s["executions"].(float64); ok {
			entry.executions = int(e)
		}
		if c, ok := s["concurrency"].(float64); ok {
			entry.concurrency = int(c)
		}
		if targets, ok := s["targets"].(map[string]any); ok {
			for _, names := range targets {
				if arr, ok := names.([]any); ok {
					entry.targets += len(arr)
				}
			}
		}
		if provides, ok := s["provides"].(map[string]any); ok {
			entry.provides = make(map[string][]string)
			for k, v := range provides {
				if arr, ok := v.([]any); ok {
					for _, val := range arr {
						if sv, ok := val.(string); ok {
							entry.provides[k] = append(entry.provides[k], sv)
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

func formatSessionStatus(e sessionEntry) string {
	if !e.connected {
		return colorDim + "●" + colorReset
	}
	if e.workerState == "draining" || e.workerState == "paused" {
		return colorYellow + "●" + colorReset
	}
	return colorGreen + "●" + colorReset
}

func formatLoad(executions, concurrency int) string {
	if concurrency > 0 {
		return fmt.Sprintf("%d/%d", executions, concurrency)
	}
	return fmt.Sprintf("%d", executions)
}

func formatProvides(provides map[string][]string) string {
	if len(provides) == 0 {
		return ""
	}
	var parts []string
	keys := make([]string, 0, len(provides))
	for k := range provides {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		parts = append(parts, k+"="+strings.Join(provides[k], ","))
	}
	return strings.Join(parts, "; ")
}

func printSessions(entries []sessionEntry) {
	if len(entries) == 0 {
		fmt.Println("No active sessions.")
		return
	}

	lines := renderSessionsTable(entries)
	for _, line := range lines {
		fmt.Println(line)
	}
}

func runSessionsList(cmd *cobra.Command, args []string) error {
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
		data, err := client.CaptureTopic(cmd.Context(), "workspaces/"+workspaceID+"/sessions")
		if err != nil {
			return err
		}
		return outputJSON(data)
	}

	if sessionsListWatch {
		return watchSessions(cmd.Context(), getHost(), isSecure(), token, getProject(), workspaceID)
	}

	data, err := client.CaptureTopic(cmd.Context(), "workspaces/"+workspaceID+"/sessions")
	if err != nil {
		return err
	}
	printSessions(parseSessionEntries(data))
	return nil
}

func watchSessions(ctx context.Context, host string, secure bool, token string, project string, workspaceID string) error {
	return watchTopics(ctx, host, secure, token, project,
		[]string{"workspaces/" + workspaceID + "/sessions"},
		func(data []map[string]any) []string {
			if data[0] == nil {
				return nil
			}
			return renderSessionsTable(parseSessionEntries(data[0]))
		},
	)
}

func renderSessionsTable(entries []sessionEntry) []string {
	headers := []string{"Session", "Pool", "Targets", "Provides", "Load"}
	var rows [][]string
	for _, e := range entries {
		pool := e.poolName
		if pool == "" {
			pool = colorDim + "(none)" + colorReset
		}
		row := []string{
			formatSessionStatus(e) + " " + e.id,
			pool,
			fmt.Sprintf("%d", e.targets),
			formatProvides(e.provides),
			formatLoad(e.executions, e.concurrency),
		}
		if !e.connected {
			for i := range row {
				row[i] = colorDim + stripAnsi(row[i]) + colorReset
			}
		}
		rows = append(rows, row)
	}
	return formatTable(headers, rows)
}

// stripAnsi removes ANSI escape codes from a string (for re-wrapping in dim).
func stripAnsi(s string) string {
	var out strings.Builder
	i := 0
	for i < len(s) {
		if s[i] == '\033' {
			// Skip escape sequence
			i++
			if i < len(s) && s[i] == '[' {
				i++
				for i < len(s) && s[i] != 'm' {
					i++
				}
				if i < len(s) {
					i++ // skip 'm'
				}
			}
		} else {
			out.WriteByte(s[i])
			i++
		}
	}
	return out.String()
}

// visibleWidth returns the display width of a string after stripping ANSI codes.
func visibleWidth(s string) int {
	return utf8.RuneCountInString(stripAnsi(s))
}

// formatTable formats headers and rows into lines (without printing).
func formatTable(headers []string, rows [][]string) []string {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			w := visibleWidth(cell)
			if i < len(widths) && w > widths[i] {
				widths[i] = w
			}
		}
	}

	var lines []string

	// Header
	var hdr strings.Builder
	for i, h := range headers {
		fmt.Fprintf(&hdr, "%-*s  ", widths[i], h)
	}
	lines = append(lines, hdr.String())

	// Rows
	for _, row := range rows {
		var line strings.Builder
		for i, cell := range row {
			if i < len(widths) {
				pad := widths[i] - visibleWidth(cell)
				if pad < 0 {
					pad = 0
				}
				fmt.Fprintf(&line, "%s%*s  ", cell, pad, "")
			}
		}
		lines = append(lines, line.String())
	}

	return lines
}
