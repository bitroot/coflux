package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var runsCmd = &cobra.Command{
	Use:   "runs",
	Short: "Manage runs",
}

func init() {
	runsCmd.AddCommand(runsInspectCmd)
	runsCmd.AddCommand(runsResultCmd)
	runsCmd.AddCommand(runsRerunCmd)
	runsCmd.AddCommand(runsCancelCmd)
}

var runsInspectCmd = &cobra.Command{
	Use:   "inspect <run-id>",
	Short: "Inspect a run",
	Long: `Inspect a run.

Displays the target, status, and execution counts of a workflow run.

Example:
  coflux runs inspect abc123
  coflux runs inspect --json abc123`,
	Args: cobra.ExactArgs(1),
	RunE: runRunsInspect,
}

var runsRerunCmd = &cobra.Command{
	Use:   "rerun <step-id>",
	Short: "Re-run a step",
	Long: `Re-run a step.

Creates a new execution attempt for the specified step.
The step ID has the format <run-id>:<step-number> (e.g., "RwD6:3").

Example:
  coflux runs rerun RwD6:3
  coflux runs rerun --json RwD6:3`,
	Args: cobra.ExactArgs(1),
	RunE: runRunsRerun,
}

var runsResultCmd = &cobra.Command{
	Use:   "result <run-id>",
	Short: "Get the result of a run",
	Long: `Get the result of a run.

Returns the JSON-formatted result of the root workflow execution.

Example:
  coflux runs result abc123`,
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

		executions, _ := s["executions"].(map[string]any)
		var latestAttempt string
		for attempt := range executions {
			if latestAttempt == "" || attempt > latestAttempt {
				latestAttempt = attempt
			}
		}
		if latestAttempt != "" {
			exec, _ = executions[latestAttempt].(map[string]any)
		}
		return
	}
	return
}

func runRunsInspect(cmd *cobra.Command, args []string) error {
	runID := args[0]

	data, _, err := captureRunTopic(cmd, runID)
	if err != nil {
		return err
	}

	if getJSON() {
		return outputJSON(data)
	}

	rootStep, rootExec := findRootStep(data)

	// Target
	if rootStep != nil {
		module, _ := rootStep["module"].(string)
		target, _ := rootStep["target"].(string)
		if module != "" {
			fmt.Printf("Target: %s.%s\n", module, target)
		}
	}

	// Created
	if createdAt, ok := data["createdAt"].(float64); ok && createdAt > 0 {
		fmt.Printf("Created: %s\n", formatTimestamp(int64(createdAt/1000)))
	}

	// Status
	if rootExec != nil {
		result, _ := rootExec["result"].(map[string]any)
		var status string
		if result == nil {
			if rootExec["assignedAt"] != nil {
				status = "running"
			} else {
				status = "pending"
			}
		} else {
			resultType, _ := result["type"].(string)
			switch resultType {
			case "value":
				status = "completed"
			case "error":
				status = "error"
			case "cancelled":
				status = "cancelled"
			case "abandoned":
				status = "abandoned"
			case "suspended":
				status = "suspended"
			default:
				status = resultType
			}
		}
		if status != "" {
			fmt.Printf("Status: %s\n", status)
		}
	}

	// Executions count
	steps, _ := data["steps"].(map[string]any)
	stepCount := len(steps)
	executionCount := 0
	for _, stepData := range steps {
		s, ok := stepData.(map[string]any)
		if !ok {
			continue
		}
		execs, _ := s["executions"].(map[string]any)
		executionCount += len(execs)
	}
	fmt.Printf("Executions: %d (%d steps)\n", executionCount, stepCount)

	return nil
}

func runRunsResult(cmd *cobra.Command, args []string) error {
	runID := args[0]

	data, _, err := captureRunTopic(cmd, runID)
	if err != nil {
		return err
	}

	_, rootExec := findRootStep(data)
	if rootExec == nil {
		return fmt.Errorf("no execution found for run %s", runID)
	}

	result, _ := rootExec["result"].(map[string]any)
	if result == nil {
		return fmt.Errorf("run %s has no result yet", runID)
	}

	if getJSON() {
		return outputJSON(result)
	}

	resultType, _ := result["type"].(string)
	switch resultType {
	case "value":
		value, _ := result["value"].(map[string]any)
		if value == nil {
			break
		}
		valueType, _ := value["type"].(string)
		switch valueType {
		case "raw":
			references, _ := value["references"].([]any)
			fmt.Println(formatData(value["data"], references))
		case "blob":
			size, _ := value["size"].(float64)
			fmt.Printf("<blob (%s)>\n", humanSize(int64(size)))
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

	if getJSON() {
		return outputJSON(map[string]any{"cancelled": true})
	}

	fmt.Println("Execution cancelled.")
	return nil
}

func runRunsRerun(cmd *cobra.Command, args []string) error {
	stepID := args[0]

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

	if getJSON() {
		return outputJSON(result)
	}

	fmt.Printf("Step re-run (execution: %s, attempt: %d).\n", result.ExecutionID, result.Attempt)
	return nil
}
