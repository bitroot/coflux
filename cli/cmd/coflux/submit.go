package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var submitCmd = &cobra.Command{
	Use:   "submit <target> [arguments...]",
	Short: "Submit a workflow to be run",
	Long: `Submit a workflow to be run.

The target should be in the format "module.target" (e.g., "myapp.workflows.process_data").
Arguments are passed as JSON strings.

Example:
  coflux submit myapp.workflows.process_data '{"key": "value"}' '123'`,
	Args: cobra.MinimumNArgs(1),
	RunE: runSubmit,
}

func runSubmit(cmd *cobra.Command, args []string) error {
	target := args[0]
	arguments := args[1:]

	workspace, err := requireWorkspace()
	if err != nil {
		return err
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	module, targetName := splitTarget(target)
	if module == "" {
		return fmt.Errorf("invalid target format: expected 'module.target', got '%s'", target)
	}

	// Get workflow definition to get options
	workflow, err := client.GetWorkflow(cmd.Context(), workspace, module, targetName)
	if err != nil {
		return fmt.Errorf("failed to get workflow: %w", err)
	}

	// Build arguments array (format: [["json", arg], ...])
	submitArgs := make([][]any, len(arguments))
	for i, arg := range arguments {
		submitArgs[i] = []any{"json", arg}
	}

	// Build options from workflow definition
	options := make(map[string]any)
	if waitFor, ok := workflow["waitFor"]; ok {
		options["waitFor"] = waitFor
	}
	if cache, ok := workflow["cache"]; ok {
		options["cache"] = cache
	}
	if defer_, ok := workflow["defer"]; ok {
		options["defer"] = defer_
	}
	if delay, ok := workflow["delay"].(float64); ok && delay > 0 {
		options["delay"] = int64(delay)
	}
	if retries, ok := workflow["retries"]; ok {
		options["retries"] = retries
	}
	if recurrent, ok := workflow["recurrent"].(bool); ok && recurrent {
		options["recurrent"] = true
	}
	if requires, ok := workflow["requires"]; ok {
		options["requires"] = requires
	}

	result, err := client.SubmitWorkflow(cmd.Context(), workspace, module, targetName, submitArgs, options)
	if err != nil {
		return fmt.Errorf("failed to submit workflow: %w", err)
	}

	if getJSON() {
		return outputJSON(result)
	}

	fmt.Printf("Workflow submitted (run: %s).\n", result.RunID)
	return nil
}
