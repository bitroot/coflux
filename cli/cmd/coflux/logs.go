package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	logclient "github.com/bitroot/coflux/cli/internal/log"
	"github.com/spf13/cobra"
	"golang.org/x/term"
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
  coflux logs -o json abc123`,
	Args: cobra.RangeArgs(1, 2),
	RunE: runLogs,
}

func init() {
	logsCmd.Flags().BoolVarP(&logsFollow, "follow", "f", false, "Stream logs in real-time")
}

// executionLabel holds the display info for a single execution in log output.
type executionLabel struct {
	Target  string // "module.target"
	StepNum string // step number within the run
	Attempt string // execution attempt number
}

// buildExecutionLabelMap builds a map from executionId to executionLabel
// using the run topic snapshot.
func buildExecutionLabelMap(data map[string]any) map[string]executionLabel {
	labels := map[string]executionLabel{}
	steps, _ := data["steps"].(map[string]any)
	for stepID, stepData := range steps {
		s, ok := stepData.(map[string]any)
		if !ok {
			continue
		}
		module, _ := s["module"].(string)
		target, _ := s["target"].(string)

		// Extract step number from stepID (format: "runID:stepNumber")
		stepNum := stepID
		if idx := strings.Index(stepID, ":"); idx >= 0 {
			stepNum = stepID[idx+1:]
		}

		executions, _ := s["executions"].(map[string]any)
		for attempt, execData := range executions {
			e, ok := execData.(map[string]any)
			if !ok {
				continue
			}
			if eid, ok := e["executionId"].(string); ok {
				labels[eid] = executionLabel{
					Target:  module + "." + target,
					StepNum: stepNum,
					Attempt: attempt,
				}
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

// interpolateTemplate replaces {key} placeholders in the template with
// formatted values. Returns the interpolated message and any extra key-value
// pairs not referenced in the template.
func interpolateTemplate(template string, values map[string]any) (string, map[string]string) {
	if len(values) == 0 {
		return template, nil
	}
	result := template
	used := map[string]bool{}
	for key, val := range values {
		placeholder := "{" + key + "}"
		if !strings.Contains(result, placeholder) {
			continue
		}
		formatted := formatLogValue(val)
		result = strings.ReplaceAll(result, placeholder, formatted)
		used[key] = true
	}
	var extras map[string]string
	for key, val := range values {
		if used[key] {
			continue
		}
		if extras == nil {
			extras = map[string]string{}
		}
		extras[key] = formatLogValue(val)
	}
	return result, extras
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

	token, err := resolveToken()
	if err != nil {
		return err
	}
	lc := logclient.NewClient(baseURL, token)

	if logsFollow {
		return runLogsFollow(cmd, lc, params, labelMap)
	}
	return runLogsQuery(cmd, lc, params, labelMap)
}

func runLogsQuery(cmd *cobra.Command, lc *logclient.Client, params url.Values, labelMap map[string]executionLabel) error {
	result, err := lc.Query(cmd.Context(), params)
	if err != nil {
		return fmt.Errorf("failed to fetch logs: %w", err)
	}

	if isOutput("json") {
		return outputJSON(result)
	}

	color := term.IsTerminal(int(os.Stdout.Fd()))
	state := logState{}
	for _, entry := range result.Logs {
		state = printLogEntry(entry, labelMap, color, state)
	}
	return nil
}

func runLogsFollow(cmd *cobra.Command, lc *logclient.Client, params url.Values, labelMap map[string]executionLabel) error {
	color := term.IsTerminal(int(os.Stdout.Fd()))
	state := logState{}
	return lc.Stream(cmd.Context(), params, func(entries []logclient.LogEntry) error {
		if isOutput("json") {
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
			state = printLogEntry(entry, labelMap, color, state)
		}
		return nil
	})
}

// logState tracks what was last printed to avoid repeating headers/dates.
type logState struct {
	execID string
	date   string
}

// printLogEntry prints a log entry, showing date and execution headers only
// when they change. Returns updated state.
func printLogEntry(entry logclient.LogEntry, labelMap map[string]executionLabel, color bool, state logState) logState {
	message, extras := interpolateTemplate(entry.Template, entry.Values)
	t := time.UnixMilli(entry.Timestamp).UTC()
	date := t.Format("2006-01-02")
	ts := t.Format("15:04:05.000")

	label, ok := labelMap[entry.ExecutionID]
	if !ok {
		label = executionLabel{Target: "exec:" + entry.ExecutionID}
	}

	extrasSuffix := formatExtras(extras, color)

	if !color {
		level := logclient.LevelName(entry.Level)
		if date != state.date {
			fmt.Printf("-- %s UTC --\n", date)
		}
		if entry.ExecutionID != state.execID {
			header := label.Target
			if label.StepNum != "" {
				header += " " + label.StepNum + ":" + label.Attempt
			}
			fmt.Printf("%s\n", header)
		}
		msgParts := message
		if message != "" && extrasSuffix != "" {
			msgParts += "  "
		}
		msgParts += extrasSuffix
		fmt.Printf("%s %-6s %s\n", ts, level, msgParts)
		return logState{execID: entry.ExecutionID, date: date}
	}

	// Print date separator when date changes
	if date != state.date {
		fmt.Printf("%s── %s UTC ──%s\n", colorDim, date, colorReset)
	}

	// Print execution header when execution changes
	if entry.ExecutionID != state.execID {
		header := label.Target
		if label.StepNum != "" {
			header += " " + colorDim + label.StepNum + ":" + label.Attempt + colorReset
		}
		fmt.Printf("%s\n", header)
	}

	icon := logLevelIcon(entry.Level)
	msgParts := message
	if message != "" && extrasSuffix != "" {
		msgParts += "  "
	}
	msgParts += extrasSuffix
	fmt.Printf("%s%s%s %s %s\n",
		colorDim, ts, colorReset,
		icon,
		msgParts,
	)
	return logState{execID: entry.ExecutionID, date: date}
}

// formatExtras formats extra key-value pairs (not in the template) as a
// dim suffix string. Keys are sorted for deterministic output.
func formatExtras(extras map[string]string, color bool) string {
	if len(extras) == 0 {
		return ""
	}
	keys := make([]string, 0, len(extras))
	for k := range extras {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, k+"="+extras[k])
	}
	joined := strings.Join(parts, " ")
	if color {
		return colorDim + joined + colorReset
	}
	return joined
}

// logLevelIcon returns a colored • for a log level.
func logLevelIcon(level int) string {
	switch level {
	case 5: // ERROR
		return colorBrightRed + "•" + colorReset
	case 4: // WARN
		return colorYellow + "•" + colorReset
	case 3: // STDERR
		return colorRed + "•" + colorReset
	case 2: // INFO
		return colorBlue + "•" + colorReset
	case 1: // STDOUT
		return "•"
	case 0: // DEBUG
		return colorDim + "•" + colorReset
	default:
		return "•"
	}
}
