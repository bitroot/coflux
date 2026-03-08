package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/bitroot/coflux/cli/internal/blob"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/term"
)

var runsCmd = &cobra.Command{
	Use:   "runs",
	Short: "Manage runs",
}

func init() {
	runsInspectCmd.Flags().BoolVar(&inspectNoWait, "no-wait", false, "Return current snapshot immediately without waiting")
	runsCmd.AddCommand(runsInspectCmd)
	runsCmd.AddCommand(runsResultCmd)
	runsCmd.AddCommand(runsRerunCmd)
	runsCmd.AddCommand(runsCancelCmd)
}

var runsInspectCmd = &cobra.Command{
	Use:   "inspect <run-id>",
	Short: "Inspect a run",
	Long: `Inspect a run.

By default, waits for the run to complete (same as submit).
Use --no-wait to return the current snapshot immediately.

Example:
  coflux runs inspect abc123
  coflux runs inspect --no-wait -o json abc123`,
	Args: cobra.ExactArgs(1),
	RunE: runRunsInspect,
}

var inspectNoWait bool
var rerunNoWait bool

var runsRerunCmd = &cobra.Command{
	Use:   "rerun <step-id>",
	Short: "Re-run a step",
	Long: `Re-run a step.

Creates a new execution attempt for the specified step.
The step ID has the format <run-id>:<step-number> (e.g., "RwD6:3").

By default, waits for the run to complete (same as submit).
Use --no-wait to re-run and exit immediately.

Example:
  coflux runs rerun RwD6:3
  coflux runs rerun --no-wait RwD6:3
  coflux runs rerun -o json RwD6:3`,
	Args: cobra.ExactArgs(1),
	RunE: runRunsRerun,
}

func init() {
	runsRerunCmd.Flags().BoolVar(&rerunNoWait, "no-wait", false, "Re-run and exit immediately without waiting")
}

var runsResultCmd = &cobra.Command{
	Use:   "result <target>",
	Short: "Get the result of a run, step, or execution",
	Long: `Get the result of a run, step, or execution.

The target can be:
  <run-id>              Result of the initial step's latest execution
  <run-id>:<step>       Result of the latest execution of a specific step
  <run-id>:<step>:<attempt>  Result of a specific execution

Example:
  coflux runs result RTbj
  coflux runs result RTbj:2
  coflux runs result RTbj:2:1`,
	Args: cobra.ExactArgs(1),
	RunE: runRunsResult,
}

// captureRunTopic resolves the workspace and captures the run topic.
func captureRunTopic(cmd *cobra.Command, runID string) (map[string]any, string, error) {
	workspace, err := requireWorkspace()
	if err != nil {
		return nil, "", err
	}

	client, err := newClient()
	if err != nil {
		return nil, "", err
	}

	workspaceID, err := resolveWorkspaceID(cmd.Context(), client, workspace)
	if err != nil {
		return nil, "", err
	}

	topicPath := fmt.Sprintf("runs/%s/%s", runID, workspaceID)
	data, err := client.CaptureTopic(cmd.Context(), topicPath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get run: %w", err)
	}
	return data, workspaceID, nil
}

// findRootStep finds the root step (no parentId) and its latest execution.
func findRootStep(data map[string]any) (step map[string]any, exec map[string]any) {
	steps, _ := data["steps"].(map[string]any)
	for _, stepData := range steps {
		s, ok := stepData.(map[string]any)
		if !ok || s["parentId"] != nil {
			continue
		}
		step = s
		exec = latestExecution(s)
		return
	}
	return
}

func runRunsInspect(cmd *cobra.Command, args []string) error {
	runID := args[0]

	// --no-wait: return current snapshot immediately
	if inspectNoWait {
		data, _, err := captureRunTopic(cmd, runID)
		if err != nil {
			return err
		}
		return outputJSON(data)
	}

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

	token, err := resolveToken()
	if err != nil {
		return fmt.Errorf("failed to resolve token: %w", err)
	}

	// -o json: wait for root step, print full run snapshot as JSON
	if isOutput("json") {
		runData, exitCode, err := waitForRootResult(cmd.Context(), getHost(), isSecure(), token, runID, workspaceID)
		if err != nil {
			return err
		}
		if runData != nil {
			outputJSON(runData)
		}
		if exitCode != 0 {
			os.Exit(exitCode)
		}
		return nil
	}

	// TTY: live tree display, wait for all steps
	if term.IsTerminal(int(os.Stdout.Fd())) {
		exitCode, err := watchRun(cmd.Context(), getHost(), isSecure(), token, runID, workspaceID)
		if err != nil {
			return err
		}
		if exitCode != 0 {
			os.Exit(exitCode)
		}
		return nil
	}

	// Non-TTY: wait for root step silently
	_, exitCode, err := waitForRootResult(cmd.Context(), getHost(), isSecure(), token, runID, workspaceID)
	if err != nil {
		return err
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}
	return nil
}

// findStepByNumber finds a step by its step number.
func findStepByNumber(data map[string]any, stepNumber string) map[string]any {
	steps, _ := data["steps"].(map[string]any)
	for _, stepData := range steps {
		s, ok := stepData.(map[string]any)
		if !ok {
			continue
		}
		sn, _ := s["stepNumber"].(float64)
		if fmt.Sprintf("%d", int(sn)) == stepNumber {
			return s
		}
	}
	return nil
}

// latestExecution returns the latest execution (highest attempt) of a step.
func latestExecution(step map[string]any) map[string]any {
	executions, _ := step["executions"].(map[string]any)
	var latestAttempt string
	for attempt := range executions {
		if latestAttempt == "" || attempt > latestAttempt {
			latestAttempt = attempt
		}
	}
	if latestAttempt != "" {
		exec, _ := executions[latestAttempt].(map[string]any)
		return exec
	}
	return nil
}

// parseResultTarget parses a target string into runID, stepNumber, attempt.
func parseResultTarget(target string) (runID, stepNumber, attempt string) {
	parts := strings.SplitN(target, ":", 3)
	runID = parts[0]
	if len(parts) >= 2 {
		stepNumber = parts[1]
	}
	if len(parts) >= 3 {
		attempt = parts[2]
	}
	return
}

// loadBlobData loads a blob's JSON data using the blob store.
func loadBlobData(key string) (any, error) {
	token, err := resolveToken()
	if err != nil {
		return nil, err
	}
	stores, err := createBlobStoresFromViper(token)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob stores: %w", err)
	}
	if len(stores) == 0 {
		return nil, fmt.Errorf("blob store not configured")
	}

	blobManager := blob.NewManager(stores, filepath.Join(os.TempDir(), "coflux-cache", "blobs"), viper.GetInt("blobs.threshold"))
	reader, err := blobManager.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to load blob: %w", err)
	}
	defer func() { _ = reader.Close() }()

	blobData, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read blob: %w", err)
	}

	var parsed any
	if err := json.Unmarshal(blobData, &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse blob JSON: %w", err)
	}
	return parsed, nil
}

func printResult(result map[string]any) error {
	resultType, _ := result["type"].(string)
	switch resultType {
	case "value":
		value, _ := result["value"].(map[string]any)
		if value == nil {
			break
		}
		valueType, _ := value["type"].(string)
		references, _ := value["references"].([]any)
		switch valueType {
		case "raw":
			fmt.Println(formatData(value["data"], references))
		case "blob":
			key, _ := value["key"].(string)
			blobData, err := loadBlobData(key)
			if err != nil {
				size, _ := value["size"].(float64)
				fmt.Printf("<blob (%s)>\n", humanSize(int64(size)))
			} else {
				fmt.Println(formatData(blobData, references))
			}
		}
	case "error":
		errData, _ := result["error"].(map[string]any)
		if errData != nil {
			frames, _ := errData["frames"].([]any)
			if len(frames) > 0 {
				fmt.Println("Traceback (most recent call last):")
				for _, f := range frames {
					frame, ok := f.(map[string]any)
					if !ok {
						continue
					}
					file, _ := frame["file"].(string)
					line, _ := frame["line"].(float64)
					name, _ := frame["name"].(string)
					code, _ := frame["code"].(string)
					fmt.Printf("  File \"%s\", line %d, in %s\n", file, int(line), name)
					if code != "" {
						fmt.Printf("    %s\n", code)
					}
				}
			}
			errType, _ := errData["type"].(string)
			errMsg, _ := errData["message"].(string)
			if errType != "" {
				fmt.Printf("%s: %s\n", errType, errMsg)
			} else {
				fmt.Println(errMsg)
			}
		}
	case "cancelled":
		fmt.Println("Cancelled")
	case "abandoned":
		fmt.Println("Abandoned")
	default:
		fmt.Println(resultType)
	}
	return nil
}

func runRunsResult(cmd *cobra.Command, args []string) error {
	target := args[0]
	runID, stepNumber, attempt := parseResultTarget(target)

	data, _, err := captureRunTopic(cmd, runID)
	if err != nil {
		return err
	}

	var exec map[string]any
	if stepNumber == "" {
		// Run ID only: get the root step's latest execution
		_, exec = findRootStep(data)
		if exec == nil {
			return fmt.Errorf("no execution found for run %s", runID)
		}
	} else if attempt == "" {
		// Run ID + step number: get latest execution of that step
		step := findStepByNumber(data, stepNumber)
		if step == nil {
			return fmt.Errorf("step %s not found in run %s", stepNumber, runID)
		}
		exec = latestExecution(step)
		if exec == nil {
			return fmt.Errorf("no execution found for step %s in run %s", stepNumber, runID)
		}
	} else {
		// Run ID + step number + attempt: get specific execution
		step := findStepByNumber(data, stepNumber)
		if step == nil {
			return fmt.Errorf("step %s not found in run %s", stepNumber, runID)
		}
		executions, _ := step["executions"].(map[string]any)
		exec, _ = executions[attempt].(map[string]any)
		if exec == nil {
			return fmt.Errorf("execution attempt %s not found for step %s in run %s", attempt, stepNumber, runID)
		}
	}

	result, _ := exec["result"].(map[string]any)
	if result == nil {
		return fmt.Errorf("no result yet")
	}

	if isOutput("json") {
		return outputJSON(result)
	}

	return printResult(result)
}

var runsCancelCmd = &cobra.Command{
	Use:   "cancel <execution-id>",
	Short: "Cancel an execution",
	Long: `Cancel an execution.

Cancels the specified execution and all of its descendants.

Example:
  coflux runs cancel 12345`,
	Args: cobra.ExactArgs(1),
	RunE: runRunsCancel,
}

func runRunsCancel(cmd *cobra.Command, args []string) error {
	executionID := args[0]

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

	if err := client.CancelExecution(cmd.Context(), workspaceID, executionID); err != nil {
		return fmt.Errorf("failed to cancel execution: %w", err)
	}

	if isOutput("json") {
		return outputJSON(map[string]any{"cancelled": true})
	}

	fmt.Println("Execution cancelled.")
	return nil
}

func runRunsRerun(cmd *cobra.Command, args []string) error {
	stepID := args[0]

	// Extract run ID from step ID (format: "runID:stepNumber")
	idx := strings.Index(stepID, ":")
	if idx < 0 {
		return fmt.Errorf("invalid step ID format: expected 'run-id:step-number', got '%s'", stepID)
	}
	runID := stepID[:idx]

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

	result, err := client.RerunStep(cmd.Context(), workspaceID, stepID)
	if err != nil {
		return fmt.Errorf("failed to rerun step: %w", err)
	}

	// --no-wait: print result and exit immediately
	if rerunNoWait {
		if isOutput("json") {
			return outputJSON(result)
		}
		fmt.Printf("Step re-run (execution: %s, attempt: %d).\n", result.ExecutionID, result.Attempt)
		return nil
	}

	token, err := resolveToken()
	if err != nil {
		return fmt.Errorf("failed to resolve token: %w", err)
	}

	// -o json: wait for root step, print full run snapshot as JSON
	if isOutput("json") {
		runData, exitCode, err := waitForRootResult(cmd.Context(), getHost(), isSecure(), token, runID, workspaceID)
		if err != nil {
			return err
		}
		if runData != nil {
			outputJSON(runData)
		}
		if exitCode != 0 {
			os.Exit(exitCode)
		}
		return nil
	}

	// TTY: live tree display, wait for all steps
	if term.IsTerminal(int(os.Stdout.Fd())) {
		exitCode, err := watchRun(cmd.Context(), getHost(), isSecure(), token, runID, workspaceID)
		if err != nil {
			return err
		}
		if exitCode != 0 {
			os.Exit(exitCode)
		}
		return nil
	}

	// Non-TTY: print execution info, wait for root step silently
	fmt.Printf("%s:%d\n", result.ExecutionID, result.Attempt)
	_, exitCode, err := waitForRootResult(cmd.Context(), getHost(), isSecure(), token, runID, workspaceID)
	if err != nil {
		return err
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}
	return nil
}
