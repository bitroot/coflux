package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var submitNoWait bool
var submitIdempotencyKey string

var submitCmd = &cobra.Command{
	Use:   "submit <target> [arguments...]",
	Short: "Submit a workflow to be run",
	Long: `Submit a workflow to be run.

The target should be in the format "module.target" (e.g., "myapp.workflows.process_data").
Arguments are passed as JSON strings.

By default, the command waits for the workflow to complete:
  - In a TTY: shows a live-updating tree of step statuses.
  - With --json: waits for the root step, then prints the result as JSON.
  - In a non-TTY: prints the run ID and waits silently (exit code reflects result).

Use --no-wait to submit and exit immediately without waiting.

Example:
  coflux submit myapp.workflows.process_data '{"key": "value"}' '123'
  coflux submit --no-wait myapp.workflows.process_data '"arg"'
  coflux submit --json myapp.workflows.process_data '"arg"'`,
	Args: cobra.MinimumNArgs(1),
	RunE: runSubmit,
}

func init() {
	submitCmd.Flags().BoolVar(&submitNoWait, "no-wait", false, "Submit and exit immediately without waiting")
	submitCmd.Flags().StringVar(&submitIdempotencyKey, "idempotency-key", "", "Idempotency key for deduplication")
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

	workspaceID, err := ensureWorkspaceID(cmd.Context(), client, workspace)
	if err != nil {
		return err
	}

	module, targetName := splitTarget(target)
	if module == "" {
		return fmt.Errorf("invalid target format: expected 'module.target', got '%s'", target)
	}

	// Get workflow definition to get options
	workflow, err := client.GetWorkflow(cmd.Context(), workspaceID, module, targetName)
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
	if submitIdempotencyKey != "" {
		options["idempotencyKey"] = submitIdempotencyKey
	}

	result, err := client.SubmitWorkflow(cmd.Context(), workspaceID, module, targetName, submitArgs, options)
	if err != nil {
		return fmt.Errorf("failed to submit workflow: %w", err)
	}

	// --no-wait: print run ID and exit immediately
	if submitNoWait {
		if getJSON() {
			return outputJSON(result)
		}
		fmt.Printf("Workflow submitted (run: %s).\n", result.RunID)
		return nil
	}

	token, err := resolveToken()
	if err != nil {
		return fmt.Errorf("failed to resolve token: %w", err)
	}

	// --json: wait for root step, print full run snapshot as JSON
	if getJSON() {
		runData, exitCode, err := waitForRootResult(cmd.Context(), getHost(), isSecure(), token, result.RunID, workspaceID)
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
		exitCode, err := watchRun(cmd.Context(), getHost(), isSecure(), token, result.RunID, workspaceID)
		if err != nil {
			return err
		}
		if exitCode != 0 {
			os.Exit(exitCode)
		}
		return nil
	}

	// Non-TTY: print run ID, wait for root step silently
	fmt.Println(result.RunID)
	_, exitCode, err := waitForRootResult(cmd.Context(), getHost(), isSecure(), token, result.RunID, workspaceID)
	if err != nil {
		return err
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}
	return nil
}
