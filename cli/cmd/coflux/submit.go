package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var submitNoWait bool
var submitIdempotencyKey string
var submitRequires []string
var submitNoRequires bool
var submitMemo bool
var submitNoMemo bool
var submitDelay float64
var submitRetries int

var submitCmd = &cobra.Command{
	Use:   "submit <target> [arguments...]",
	Short: "Submit a workflow to be run",
	Long: `Submit a workflow to be run.

The target should be in the format "module/target" (e.g., "myapp.workflows/process_data").
Arguments are passed as JSON strings.

By default, the command waits for the workflow to complete:
  - In a TTY: shows a live-updating tree of step statuses.
  - With -o json: waits for the root step, then prints the result as JSON.
  - In a non-TTY: prints the run ID and waits silently (exit code reflects result).

Use --no-wait to submit and exit immediately without waiting.

Configuration from the workflow definition (requires, memo, delay, retries) can
be overridden for this run via flags. Passing --requires replaces the workflow's
requires entirely (it does not append).

Example:
  coflux submit myapp.workflows/process_data '{"key": "value"}' '123'
  coflux submit --no-wait myapp.workflows/process_data '"arg"'
  coflux submit -o json myapp.workflows/process_data '"arg"'
  coflux submit --requires gpu:A100 --requires region:eu myapp.workflows/train '"x"'
  coflux submit --no-memo --retries 0 myapp.workflows/process_data '"x"'`,
	Args: cobra.MinimumNArgs(1),
	RunE: runSubmit,
}

func init() {
	submitCmd.Flags().BoolVar(&submitNoWait, "no-wait", false, "Submit and exit immediately without waiting")
	submitCmd.Flags().StringVar(&submitIdempotencyKey, "idempotency-key", "", "Idempotency key for deduplication")
	submitCmd.Flags().StringSliceVar(&submitRequires, "requires", nil, "Override the workflow's requires (e.g., --requires gpu:A100,gpu:H100 --requires region:eu). Replaces the workflow's requires entirely.")
	submitCmd.Flags().BoolVar(&submitNoRequires, "no-requires", false, "Override the workflow's requires with an empty set")
	submitCmd.Flags().BoolVar(&submitMemo, "memo", false, "Override the workflow to enable memoisation")
	submitCmd.Flags().BoolVar(&submitNoMemo, "no-memo", false, "Override the workflow to disable memoisation")
	submitCmd.Flags().Float64Var(&submitDelay, "delay", 0, "Override the workflow's delay (seconds)")
	submitCmd.Flags().IntVar(&submitRetries, "retries", 0, "Override the workflow's retry limit (0 = no retries)")
	submitCmd.MarkFlagsMutuallyExclusive("requires", "no-requires")
	submitCmd.MarkFlagsMutuallyExclusive("memo", "no-memo")
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
		return fmt.Errorf("invalid target format: expected 'module/target', got '%s'", target)
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
	if memo, ok := workflow["memo"]; ok {
		options["memo"] = memo
	}
	if timeout, ok := workflow["timeout"].(float64); ok && timeout > 0 {
		options["timeout"] = int64(timeout)
	}
	if streams, ok := workflow["streams"].(map[string]any); ok && streams != nil {
		options["streams"] = streams
	}

	// Apply per-run overrides from flags.
	if cmd.Flags().Changed("requires") {
		options["requires"] = parseProvides(submitRequires)
	} else if submitNoRequires {
		options["requires"] = map[string][]string{}
	}
	if submitMemo {
		options["memo"] = true
	} else if submitNoMemo {
		options["memo"] = false
	}
	if cmd.Flags().Changed("delay") {
		options["delay"] = int64(submitDelay * 1000)
	}
	if cmd.Flags().Changed("retries") {
		if submitRetries <= 0 {
			options["retries"] = nil
		} else {
			options["retries"] = map[string]any{
				"limit":      submitRetries,
				"backoffMin": 0,
				"backoffMax": 0,
			}
		}
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
		if isOutput("json") {
			return outputJSON(result)
		}
		fmt.Printf("Workflow submitted (run: %s).\n", result.RunID)
		return nil
	}

	token, err := resolveToken()
	if err != nil {
		return fmt.Errorf("failed to resolve token: %w", err)
	}

	// -o json: wait for root step, print full run snapshot as JSON
	if isOutput("json") {
		runData, exitCode, err := waitForRootResult(cmd.Context(), getHost(), isSecure(), token, getProject(), result.RunID, workspaceID)
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
		exitCode, err := watchRun(cmd.Context(), getHost(), isSecure(), token, getProject(), result.RunID, workspaceID)
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
	_, exitCode, err := waitForRootResult(cmd.Context(), getHost(), isSecure(), token, getProject(), result.RunID, workspaceID)
	if err != nil {
		return err
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}
	return nil
}
