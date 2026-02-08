package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	logclient "github.com/bitroot/coflux/cli/internal/log"
	"github.com/spf13/cobra"
)

var (
	logsFollow bool
)

var logsCmd = &cobra.Command{
	Use:   "logs <run-id> [step-id:attempt]",
	Short: "Fetch logs for a run",
	Long: `Fetch and display log entries for a run.

Optionally filter to a single execution by providing [step-id:attempt].
Supports a --follow flag for real-time SSE streaming.

Examples:
  coflux logs abc123
  coflux logs abc123 --follow
  coflux logs abc123 SXw9K2p:1
  coflux logs --json abc123`,
	Args: cobra.RangeArgs(1, 2),
	RunE: runLogs,
}

func init() {
	logsCmd.Flags().BoolVarP(&logsFollow, "follow", "f", false, "Stream logs in real-time")
}

// buildExecutionLabelMap builds a map from executionId to "module.target" label
// using the run topic snapshot.
func buildExecutionLabelMap(data map[string]any) map[string]string {
	labels := map[string]string{}
	steps, _ := data["steps"].(map[string]any)
	for _, stepData := range steps {
		s, ok := stepData.(map[string]any)
		if !ok {
			continue
		}
		module, _ := s["module"].(string)
		target, _ := s["target"].(string)
		label := module + "." + target

		executions, _ := s["executions"].(map[string]any)
		for _, execData := range executions {
			e, ok := execData.(map[string]any)
			if !ok {
				continue
			}
			if eid, ok := e["executionId"].(string); ok {
				labels[eid] = label
			}
		}
	}
	return labels
}

// resolveStepAttempt looks up the execution ID for a "stepId:attempt" string
// using the run topic snapshot. Step IDs are compound (e.g., "RwD6:2"), so the
// full format is "RwD6:2:1" (run_id:step_number:attempt). We split on the last
// colon to separate the step ID from the attempt.
func resolveStepAttempt(data map[string]any, stepAttempt string) (string, error) {
	lastColon := strings.LastIndex(stepAttempt, ":")
	if lastColon < 0 {
		return "", fmt.Errorf("invalid step:attempt format %q (expected step-id:attempt)", stepAttempt)
	}
	stepID := stepAttempt[:lastColon]
	attempt := stepAttempt[lastColon+1:]

	steps, _ := data["steps"].(map[string]any)
	stepData, ok := steps[stepID]
	if !ok {
		return "", fmt.Errorf("step %q not found in run", stepID)
	}
	s, ok := stepData.(map[string]any)
	if !ok {
		return "", fmt.Errorf("step %q has invalid data", stepID)
	}
	executions, _ := s["executions"].(map[string]any)
	execData, ok := executions[attempt]
	if !ok {
		return "", fmt.Errorf("attempt %s not found for step %q", attempt, stepID)
	}
	e, ok := execData.(map[string]any)
	if !ok {
		return "", fmt.Errorf("execution data for step %q attempt %s is invalid", stepID, attempt)
	}
	if eid, ok := e["executionId"].(string); ok {
		return eid, nil
	}
	return "", fmt.Errorf("no executionId found for step %q attempt %s", stepID, attempt)
}

// interpolateTemplate replaces {key} placeholders in the template with formatted values.
func interpolateTemplate(template string, values map[string]any) string {
	if len(values) == 0 {
		return template
	}
	result := template
	for key, val := range values {
		placeholder := "{" + key + "}"
		if !strings.Contains(result, placeholder) {
			continue
		}
		formatted := formatLogValue(val)
		result = strings.ReplaceAll(result, placeholder, formatted)
	}
	return result
}

// formatLogValue formats a single log value entry.
// The value is expected to be {"type": "raw", "data": ..., "references": [...]}
// or {"type": "blob", "key": "...", "size": N, "references": [...]}.
func formatLogValue(val any) string {
	v, ok := val.(map[string]any)
	if !ok {
		return fmt.Sprintf("%v", val)
	}
	typ, _ := v["type"].(string)
	switch typ {
	case "raw":
		references, _ := v["references"].([]any)
		return formatLogData(v["data"], references)
	case "blob":
		size, _ := v["size"].(float64)
		return fmt.Sprintf("<blob (%s)>", humanSize(int64(size)))
	}
	return fmt.Sprintf("%v", val)
}

func runLogs(cmd *cobra.Command, args []string) error {
	runID := args[0]

	// Capture run topic for labels and step:attempt resolution
	data, workspaceID, err := captureRunTopic(cmd, runID)
	if err != nil {
		return err
	}

	labelMap := buildExecutionLabelMap(data)

	host := getHost()
	secure := isSecure()
	scheme := "http"
	if secure {
		scheme = "https"
	}
	baseURL := fmt.Sprintf("%s://%s/logs", scheme, host)

	// Build query params
	params := url.Values{}
	params.Set("run", runID)
	params.Set("workspaces", workspaceID)

	// Resolve optional step:attempt positional arg
	if len(args) > 1 {
		executionID, err := resolveStepAttempt(data, args[1])
		if err != nil {
			return err
		}
		params.Set("execution", executionID)
	}

	lc := logclient.NewClient(baseURL)

	if logsFollow {
		return runLogsFollow(cmd, lc, params, labelMap)
	}
	return runLogsQuery(cmd, lc, params, labelMap)
}

func runLogsQuery(cmd *cobra.Command, lc *logclient.Client, params url.Values, labelMap map[string]string) error {
	result, err := lc.Query(cmd.Context(), params)
	if err != nil {
		return fmt.Errorf("failed to fetch logs: %w", err)
	}

	if getJSON() {
		return outputJSON(result)
	}

	for _, entry := range result.Logs {
		printLogEntry(entry, labelMap)
	}
	return nil
}

func runLogsFollow(cmd *cobra.Command, lc *logclient.Client, params url.Values, labelMap map[string]string) error {
	return lc.Stream(cmd.Context(), params, func(entries []logclient.LogEntry) error {
		if getJSON() {
			for _, entry := range entries {
				data, err := json.Marshal(entry)
				if err != nil {
					return err
				}
				fmt.Println(string(data))
			}
			return nil
		}
		for _, entry := range entries {
			printLogEntry(entry, labelMap)
		}
		return nil
	})
}

func printLogEntry(entry logclient.LogEntry, labelMap map[string]string) {
	ts := time.UnixMilli(entry.Timestamp).UTC().Format(time.RFC3339Nano)
	level := logclient.LevelName(entry.Level)

	label := labelMap[entry.ExecutionID]
	if label == "" {
		label = "exec:" + entry.ExecutionID
	}

	message := interpolateTemplate(entry.Template, entry.Values)

	fmt.Printf("%s %-6s [%s] %s\n", ts, level, label, message)
}
