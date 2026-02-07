package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var tokensCmd = &cobra.Command{
	Use:   "tokens",
	Short: "Manage API tokens",
}

func init() {
	tokensCmd.AddCommand(tokensListCmd)
	tokensCmd.AddCommand(tokensCreateCmd)
	tokensCmd.AddCommand(tokensRevokeCmd)
}

// tokens list
var tokensListCmd = &cobra.Command{
	Use:   "list",
	Short: "List API tokens",
	RunE:  runTokensList,
}

func runTokensList(cmd *cobra.Command, args []string) error {
	client, err := newClient()
	if err != nil {
		return err
	}

	tokens, err := client.ListTokens(cmd.Context())
	if err != nil {
		return err
	}

	if len(tokens) == 0 {
		fmt.Println("No tokens found.")
		return nil
	}

	var rows [][]string
	for _, t := range tokens {
		externalID := getString(t, "externalId")
		name := getString(t, "name")
		if name == "" {
			name = "(unnamed)"
		}
		createdAt := getInt64(t, "createdAt")
		revokedAt := getInt64(t, "revokedAt")
		expiresAt := getInt64(t, "expiresAt")

		status := "active"
		if revokedAt > 0 {
			status = "revoked"
		} else if expiresAt > 0 && expiresAt < time.Now().Unix() {
			status = "expired"
		}

		workspaces := "(all)"
		if ws, ok := t["workspaces"].([]any); ok && len(ws) > 0 {
			var wsList []string
			for _, w := range ws {
				if s, ok := w.(string); ok {
					wsList = append(wsList, s)
				}
			}
			workspaces = strings.Join(wsList, ", ")
		}

		rows = append(rows, []string{
			externalID,
			name,
			formatTimestamp(createdAt),
			status,
			workspaces,
		})
	}

	printTable([]string{"ID", "Name", "Created", "Status", "Workspaces"}, rows)
	return nil
}

// tokens create
var (
	tokensCreateName       string
	tokensCreateWorkspaces string
)

var tokensCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create an API token",
	Long: `Create a new API token.

The token value is displayed only once. Make sure to copy it.

Use --workspaces to restrict the token to specific workspaces. Patterns can
include wildcards (e.g., 'development/*' matches all workspaces starting with
'development/'). If omitted, the token has access to all workspaces.`,
	RunE: runTokensCreate,
}

func init() {
	tokensCreateCmd.Flags().StringVar(&tokensCreateName, "name", "", "Name for the token")
	tokensCreateCmd.Flags().StringVar(&tokensCreateWorkspaces, "workspaces", "", "Comma-separated workspace patterns")
}

func runTokensCreate(cmd *cobra.Command, args []string) error {
	client, err := newClient()
	if err != nil {
		return err
	}

	var workspaces []string
	if tokensCreateWorkspaces != "" {
		for _, w := range strings.Split(tokensCreateWorkspaces, ",") {
			w = strings.TrimSpace(w)
			if w != "" {
				workspaces = append(workspaces, w)
			}
		}
	}

	newToken, err := client.CreateToken(cmd.Context(), tokensCreateName, workspaces)
	if err != nil {
		return err
	}

	fmt.Println("Token created successfully.")
	fmt.Println()
	fmt.Printf("Token: %s\n", newToken)
	if len(workspaces) > 0 {
		fmt.Printf("Workspaces: %s\n", tokensCreateWorkspaces)
	} else {
		fmt.Println("Workspaces: (all)")
	}
	fmt.Println()
	fmt.Println("Copy this now, it won't be shown again.")
	return nil
}

// tokens revoke
var tokensRevokeCmd = &cobra.Command{
	Use:   "revoke <token-id>",
	Short: "Revoke an API token",
	Args:  cobra.ExactArgs(1),
	RunE:  runTokensRevoke,
}

func runTokensRevoke(cmd *cobra.Command, args []string) error {
	tokenID := args[0]

	client, err := newClient()
	if err != nil {
		return err
	}

	// Verify token exists
	tokens, err := client.ListTokens(cmd.Context())
	if err != nil {
		return err
	}

	var found bool
	var alreadyRevoked bool
	for _, t := range tokens {
		if getString(t, "externalId") == tokenID {
			found = true
			if getInt64(t, "revokedAt") > 0 {
				alreadyRevoked = true
			}
			break
		}
	}

	if !found {
		return fmt.Errorf("token '%s' not found", tokenID)
	}
	if alreadyRevoked {
		return fmt.Errorf("token '%s' is already revoked", tokenID)
	}

	if err := client.RevokeToken(cmd.Context(), tokenID); err != nil {
		return err
	}

	fmt.Printf("Token '%s' has been revoked.\n", tokenID)
	return nil
}

func getInt64(m map[string]any, key string) int64 {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case float64:
			return int64(n)
		case int64:
			return n
		case int:
			return int64(n)
		}
	}
	return 0
}
