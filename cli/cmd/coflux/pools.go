package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var poolsCmd = &cobra.Command{
	Use:   "pools",
	Short: "Manage pools",
}

func init() {
	poolsCmd.AddCommand(poolsListCmd)
	poolsCmd.AddCommand(poolsUpdateCmd)
	poolsCmd.AddCommand(poolsDeleteCmd)
}

// pools list
var poolsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List pools",
	RunE:  runPoolsList,
}

func runPoolsList(cmd *cobra.Command, args []string) error {
	workspace, err := requireWorkspace()
	if err != nil {
		return err
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	pools, err := client.GetPools(cmd.Context(), workspace)
	if err != nil {
		return err
	}

	if len(pools) == 0 {
		fmt.Println("No pools found.")
		return nil
	}

	var rows [][]string
	for name, pool := range pools {
		launcher := getString(pool, "launcherType")
		modules := ""
		if m, ok := pool["modules"].([]any); ok {
			var mods []string
			for _, mod := range m {
				if s, ok := mod.(string); ok {
					mods = append(mods, s)
				}
			}
			modules = strings.Join(mods, ",")
		}
		provides := encodeProvides(pool["provides"])
		rows = append(rows, []string{name, launcher, modules, provides})
	}

	printTable([]string{"Name", "Launcher", "Modules", "Provides"}, rows)
	return nil
}

// pools update
var (
	poolsUpdateModules     []string
	poolsUpdateProvides    []string
	poolsUpdateDockerImage string
	poolsUpdateDockerHost  string
)

var poolsUpdateCmd = &cobra.Command{
	Use:   "update <name>",
	Short: "Update a pool",
	Args:  cobra.ExactArgs(1),
	RunE:  runPoolsUpdate,
}

func init() {
	poolsUpdateCmd.Flags().StringSliceVarP(&poolsUpdateModules, "module", "m", nil, "Modules to be hosted")
	poolsUpdateCmd.Flags().StringSliceVar(&poolsUpdateProvides, "provides", nil, "Features that workers provide")
	poolsUpdateCmd.Flags().StringVar(&poolsUpdateDockerImage, "docker-image", "", "Docker image")
	poolsUpdateCmd.Flags().StringVar(&poolsUpdateDockerHost, "docker-host", "", "Docker host")
}

func runPoolsUpdate(cmd *cobra.Command, args []string) error {
	name := args[0]

	workspace, err := requireWorkspace()
	if err != nil {
		return err
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	// Get existing pool
	pool, _ := client.GetPool(cmd.Context(), workspace, name)
	if pool == nil {
		pool = make(map[string]any)
	}

	// Apply updates
	if poolsUpdateModules != nil {
		pool["modules"] = poolsUpdateModules
	}
	if poolsUpdateProvides != nil {
		pool["provides"] = parseProvides(poolsUpdateProvides)
	}
	if poolsUpdateDockerImage != "" || poolsUpdateDockerHost != "" {
		launcher, ok := pool["launcher"].(map[string]any)
		if !ok || getString(launcher, "type") != "docker" {
			launcher = map[string]any{"type": "docker"}
		}
		if poolsUpdateDockerImage != "" {
			launcher["image"] = poolsUpdateDockerImage
		}
		if poolsUpdateDockerHost != "" {
			launcher["dockerHost"] = poolsUpdateDockerHost
		}
		pool["launcher"] = launcher
	}

	if err := client.UpdatePool(cmd.Context(), workspace, name, pool); err != nil {
		return err
	}

	fmt.Printf("Updated pool '%s'.\n", name)
	return nil
}

// pools delete
var poolsDeleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a pool",
	Args:  cobra.ExactArgs(1),
	RunE:  runPoolsDelete,
}

func runPoolsDelete(cmd *cobra.Command, args []string) error {
	name := args[0]

	workspace, err := requireWorkspace()
	if err != nil {
		return err
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	if err := client.UpdatePool(cmd.Context(), workspace, name, nil); err != nil {
		return err
	}

	fmt.Printf("Deleted pool '%s'.\n", name)
	return nil
}

// Helper functions

func encodeProvides(provides any) string {
	if provides == nil {
		return ""
	}
	m, ok := provides.(map[string]any)
	if !ok {
		return ""
	}
	var parts []string
	for k, v := range m {
		if values, ok := v.([]any); ok {
			var vals []string
			for _, val := range values {
				if s, ok := val.(string); ok {
					vals = append(vals, s)
				}
			}
			parts = append(parts, fmt.Sprintf("%s:%s", k, strings.Join(vals, ",")))
		}
	}
	return strings.Join(parts, " ")
}

func parseProvides(args []string) map[string][]string {
	result := make(map[string][]string)
	for _, arg := range args {
		for _, part := range strings.Split(arg, ";") {
			if part == "" {
				continue
			}
			var key, values string
			if k, v, ok := strings.Cut(part, ":"); ok {
				key = k
				values = v
			} else {
				key = part
				values = "true"
			}
			result[key] = append(result[key], strings.Split(values, ",")...)
		}
	}
	return result
}
