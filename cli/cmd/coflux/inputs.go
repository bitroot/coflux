package main

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
)

var inputsCmd = &cobra.Command{
	Use:   "inputs",
	Short: "Manage inputs",
}

func init() {
	inputsCmd.AddCommand(inputsListCmd)
	inputsCmd.AddCommand(inputsInspectCmd)
	inputsCmd.AddCommand(inputsRespondCmd)
	inputsCmd.AddCommand(inputsDismissCmd)
}

// inputs list

var inputsListRun string

var inputsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List pending inputs",
	Long:  `List inputs with active dependencies (executions waiting for a response).`,
	RunE:  runInputsList,
}

func init() {
	inputsListCmd.Flags().StringVar(&inputsListRun, "run", "", "Filter by run ID")
}

func runInputsList(cmd *cobra.Command, args []string) error {
	client, err := newClient()
	if err != nil {
		return err
	}

	wsName, err := requireWorkspace()
	if err != nil {
		return err
	}

	wsID, err := resolveWorkspaceID(cmd.Context(), client, wsName)
	if err != nil {
		return err
	}

	data, err := client.CaptureTopic(cmd.Context(), "workspaces/"+wsID+"/inputs")
	if err != nil {
		return err
	}

	// Filter by run if requested
	if inputsListRun != "" {
		filtered := make(map[string]any)
		for id, v := range data {
			if entry, ok := v.(map[string]any); ok {
				if getString(entry, "runId") == inputsListRun {
					filtered[id] = v
				}
			}
		}
		data = filtered
	}

	if isOutput("json") {
		return outputJSON(data)
	}

	if len(data) == 0 {
		fmt.Println("No pending inputs.")
		return nil
	}

	var rows [][]string
	for id, v := range data {
		entry, ok := v.(map[string]any)
		if !ok {
			continue
		}
		runID := getString(entry, "runId")
		executionID := getString(entry, "executionId")
		key := getString(entry, "key")
		createdAt := getInt64(entry, "createdAt")

		if key == "" {
			key = "-"
		}

		rows = append(rows, []string{
			id,
			runID,
			executionID,
			key,
			formatTimestamp(createdAt),
		})
	}

	printTable([]string{"ID", "Run", "Execution", "Key", "Created"}, rows)
	return nil
}

// inputs inspect

var inputsInspectCmd = &cobra.Command{
	Use:   "inspect <input-id>",
	Short: "Inspect an input",
	Args:  cobra.ExactArgs(1),
	RunE:  runInputsInspect,
}

func runInputsInspect(cmd *cobra.Command, args []string) error {
	inputID := args[0]

	client, err := newClient()
	if err != nil {
		return err
	}

	input, err := client.GetInput(cmd.Context(), inputID)
	if err != nil {
		return err
	}

	if isOutput("json") {
		return outputJSON(input)
	}

	fmt.Printf("Input: %s\n", inputID)

	if key := getString(input, "key"); key != "" {
		fmt.Printf("Key: %s\n", key)
	}

	fmt.Printf("Created: %s\n", formatTimestamp(getInt64(input, "createdAt")))

	// Display prompt
	template := getString(input, "template")
	if template != "" {
		fmt.Printf("\nPrompt: %s\n", template)
	}

	// Display schema
	if schema, ok := input["schema"].(string); ok && schema != "" {
		// Pretty-print the JSON schema
		var parsed any
		if json.Unmarshal([]byte(schema), &parsed) == nil {
			pretty, err := json.MarshalIndent(parsed, "  ", "  ")
			if err == nil {
				fmt.Printf("Schema:\n  %s\n", string(pretty))
			} else {
				fmt.Printf("Schema: %s\n", schema)
			}
		} else {
			fmt.Printf("Schema: %s\n", schema)
		}
	}

	// Display response status
	if resp, ok := input["response"].(map[string]any); ok && resp != nil {
		respType := getString(resp, "type")
		respondedAt := getInt64(resp, "createdAt")
		fmt.Printf("\nStatus: %s\n", respType)
		fmt.Printf("Responded: %s\n", formatTimestamp(respondedAt))

		if respType == "value" {
			if value, ok := resp["value"]; ok {
				valueJSON, err := json.MarshalIndent(value, "  ", "  ")
				if err == nil {
					fmt.Printf("Value:\n  %s\n", string(valueJSON))
				}
			}
		}
	} else {
		fmt.Printf("\nStatus: pending\n")
	}

	return nil
}

// inputs respond

var inputsRespondCmd = &cobra.Command{
	Use:   "respond <input-id> <json-value>",
	Short: "Respond to an input",
	Long:  `Submit a JSON value as the response to an input request.`,
	Args:  cobra.ExactArgs(2),
	RunE:  runInputsRespond,
}

func runInputsRespond(cmd *cobra.Command, args []string) error {
	inputID := args[0]
	valueStr := args[1]

	var value any
	if err := json.Unmarshal([]byte(valueStr), &value); err != nil {
		return fmt.Errorf("invalid JSON value: %w", err)
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	if err := client.RespondInput(cmd.Context(), inputID, value); err != nil {
		return err
	}

	if isOutput("json") {
		return outputJSON(map[string]any{"responded": true})
	}

	fmt.Printf("Input '%s' responded.\n", inputID)
	return nil
}

// inputs dismiss

var inputsDismissCmd = &cobra.Command{
	Use:   "dismiss <input-id>",
	Short: "Dismiss an input",
	Args:  cobra.ExactArgs(1),
	RunE:  runInputsDismiss,
}

func runInputsDismiss(cmd *cobra.Command, args []string) error {
	inputID := args[0]

	client, err := newClient()
	if err != nil {
		return err
	}

	if err := client.DismissInput(cmd.Context(), inputID); err != nil {
		return err
	}

	if isOutput("json") {
		return outputJSON(map[string]any{"dismissed": true})
	}

	fmt.Printf("Input '%s' dismissed.\n", inputID)
	return nil
}
