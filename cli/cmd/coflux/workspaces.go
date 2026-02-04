package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var workspacesCmd = &cobra.Command{
	Use:   "workspaces",
	Short: "Manage workspaces",
}

func init() {
	workspacesCmd.AddCommand(workspacesListCmd)
	workspacesCmd.AddCommand(workspacesCreateCmd)
	workspacesCmd.AddCommand(workspacesUpdateCmd)
	workspacesCmd.AddCommand(workspacesArchiveCmd)
}

// workspaces list
var workspacesListCmd = &cobra.Command{
	Use:   "list",
	Short: "List workspaces",
	RunE:  runWorkspacesList,
}

func runWorkspacesList(cmd *cobra.Command, args []string) error {
	client, err := newClient()
	if err != nil {
		return err
	}

	workspaces, err := client.GetWorkspaces(cmd.Context())
	if err != nil {
		return err
	}

	if len(workspaces) == 0 {
		fmt.Println("No workspaces found.")
		return nil
	}

	var rows [][]string
	for _, ws := range workspaces {
		name := getString(ws, "name")
		baseID := getString(ws, "baseId")
		baseName := "(None)"
		if baseID != "" {
			if base, ok := workspaces[baseID]; ok {
				baseName = getString(base, "name")
			}
		}
		rows = append(rows, []string{name, baseName})
	}

	printTable([]string{"Name", "Base"}, rows)
	return nil
}

// workspaces create
var wsCreateBase string

var workspacesCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a workspace",
	Args:  cobra.ExactArgs(1),
	RunE:  runWorkspacesCreate,
}

func init() {
	workspacesCreateCmd.Flags().StringVar(&wsCreateBase, "base", "", "Base workspace to inherit from")
}

func runWorkspacesCreate(cmd *cobra.Command, args []string) error {
	name := args[0]

	client, err := newClient()
	if err != nil {
		return err
	}

	var baseID *string
	if wsCreateBase != "" {
		workspaces, err := client.GetWorkspaces(cmd.Context())
		if err != nil {
			return err
		}
		for id, ws := range workspaces {
			if getString(ws, "name") == wsCreateBase {
				baseID = &id
				break
			}
		}
		if baseID == nil {
			return fmt.Errorf("base workspace not found: %s", wsCreateBase)
		}
	}

	if err := client.CreateWorkspace(cmd.Context(), name, baseID); err != nil {
		return err
	}

	fmt.Printf("Created workspace '%s'.\n", name)
	return nil
}

// workspaces update
var (
	wsUpdateName   string
	wsUpdateBase   string
	wsUpdateNoBase bool
)

var workspacesUpdateCmd = &cobra.Command{
	Use:   "update",
	Short: "Update a workspace",
	RunE:  runWorkspacesUpdate,
}

func init() {
	workspacesUpdateCmd.Flags().StringVar(&wsUpdateName, "name", "", "New name for the workspace")
	workspacesUpdateCmd.Flags().StringVar(&wsUpdateBase, "base", "", "New base workspace")
	workspacesUpdateCmd.Flags().BoolVar(&wsUpdateNoBase, "no-base", false, "Unset the base workspace")
}

func runWorkspacesUpdate(cmd *cobra.Command, args []string) error {
	workspace, err := requireWorkspace()
	if err != nil {
		return err
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	workspaces, err := client.GetWorkspaces(cmd.Context())
	if err != nil {
		return err
	}

	var workspaceID string
	for id, ws := range workspaces {
		if getString(ws, "name") == workspace {
			workspaceID = id
			break
		}
	}
	if workspaceID == "" {
		return fmt.Errorf("workspace not found: %s", workspace)
	}

	updates := make(map[string]any)
	if wsUpdateName != "" {
		updates["name"] = wsUpdateName
	}
	if wsUpdateBase != "" {
		var baseID string
		for id, ws := range workspaces {
			if getString(ws, "name") == wsUpdateBase {
				baseID = id
				break
			}
		}
		if baseID == "" {
			return fmt.Errorf("base workspace not found: %s", wsUpdateBase)
		}
		updates["baseId"] = baseID
	} else if wsUpdateNoBase {
		updates["baseId"] = nil
	}

	if err := client.UpdateWorkspace(cmd.Context(), workspaceID, updates); err != nil {
		return err
	}

	displayName := wsUpdateName
	if displayName == "" {
		displayName = workspace
	}
	fmt.Printf("Updated workspace '%s'.\n", displayName)
	return nil
}

// workspaces archive
var workspacesArchiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Archive a workspace",
	RunE:  runWorkspacesArchive,
}

func runWorkspacesArchive(cmd *cobra.Command, args []string) error {
	workspace, err := requireWorkspace()
	if err != nil {
		return err
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	workspaces, err := client.GetWorkspaces(cmd.Context())
	if err != nil {
		return err
	}

	var workspaceID string
	for id, ws := range workspaces {
		if getString(ws, "name") == workspace {
			workspaceID = id
			break
		}
	}
	if workspaceID == "" {
		return fmt.Errorf("workspace not found: %s", workspace)
	}

	if err := client.ArchiveWorkspace(cmd.Context(), workspaceID); err != nil {
		return err
	}

	fmt.Printf("Archived workspace '%s'.\n", workspace)
	return nil
}

func getString(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
