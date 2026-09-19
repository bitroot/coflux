package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/spf13/cobra"
)

var secretsCmd = &cobra.Command{
	Use:   "secrets",
	Short: "Manage secrets",
	Long: `Manage secrets: values that pools refer to by name - a launcher's credentials,
or environment variables for workers - and that never appear in a pool's
configuration.

A secret is set for a scope: a workspace name, or a prefix of one. A secret for
'development' applies to 'development/joe', and the nearest scope wins. Without
--scope or --global, the scope is the current workspace.`,
}

var (
	secretsScope    string
	secretsGlobal   bool
	secretsFromEnv  string
	secretsFromFile string
)

func init() {
	for _, cmd := range []*cobra.Command{secretsSetCmd, secretsDeleteCmd} {
		cmd.Flags().StringVar(&secretsScope, "scope", "", "Workspace name, or prefix, the secret applies to (default: the current workspace)")
		cmd.Flags().BoolVar(&secretsGlobal, "global", false, "Apply to every workspace in the project")
	}
	secretsSetCmd.Flags().StringVar(&secretsFromEnv, "from-env", "", "Read the value from this environment variable")
	secretsSetCmd.Flags().StringVar(&secretsFromFile, "from-file", "", "Read the value from this file")
	secretsCmd.AddCommand(secretsSetCmd, secretsListCmd, secretsDeleteCmd)
}

// secretScope is the scope a secret is set or deleted in: "" for the whole
// project, else a workspace name or prefix.
func secretScope() (string, error) {
	if secretsGlobal && secretsScope != "" {
		return "", fmt.Errorf("--scope and --global can't both be given")
	}
	if secretsGlobal {
		return "", nil
	}
	if secretsScope != "" {
		return secretsScope, nil
	}
	return requireWorkspace()
}

func describeScope(scope string) string {
	if scope == "" {
		return "all workspaces"
	}
	return fmt.Sprintf("'%s'", scope)
}

// secrets set

var secretsSetCmd = &cobra.Command{
	Use:   "set <name>",
	Short: "Set a secret's value",
	Long: `Set a secret's value, creating it or replacing what it had.

The value is read from stdin, so it never appears on the command line or in
shell history:

  printf '%s' "$API_KEY" | coflux secrets set api-key
  aws configure export-credentials --profile sandbox | coflux secrets set aws-sandbox

Or from an environment variable (--from-env) or a file (--from-file). A single
trailing newline is dropped, so 'echo' works too.`,
	Args: cobra.ExactArgs(1),
	RunE: runSecretsSet,
}

func runSecretsSet(cmd *cobra.Command, args []string) error {
	name := args[0]

	scope, err := secretScope()
	if err != nil {
		return err
	}

	value, err := readSecretValue()
	if err != nil {
		return err
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	result, err := client.SetSecret(cmd.Context(), scope, name, value)
	if err != nil {
		return err
	}

	fmt.Printf("Set secret '%s' (version %d) for %s.\n", result.Name, result.Version, describeScope(result.Scope))
	return nil
}

func readSecretValue() (string, error) {
	if secretsFromEnv != "" && secretsFromFile != "" {
		return "", fmt.Errorf("--from-env and --from-file can't both be given")
	}

	var data []byte
	switch {
	case secretsFromEnv != "":
		value, ok := os.LookupEnv(secretsFromEnv)
		if !ok {
			return "", fmt.Errorf("environment variable %s is not set", secretsFromEnv)
		}
		return value, nil
	case secretsFromFile != "":
		read, err := os.ReadFile(secretsFromFile)
		if err != nil {
			return "", err
		}
		data = read
	default:
		if info, err := os.Stdin.Stat(); err == nil && info.Mode()&os.ModeCharDevice != 0 {
			fmt.Fprintln(os.Stderr, "Enter the value, then press Ctrl-D:")
		}
		read, err := io.ReadAll(os.Stdin)
		if err != nil {
			return "", err
		}
		data = read
	}

	data = bytes.TrimSuffix(data, []byte("\n"))
	data = bytes.TrimSuffix(data, []byte("\r"))
	if len(data) == 0 {
		return "", fmt.Errorf("no value given")
	}
	return string(data), nil
}

// secrets list

var secretsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List secrets",
	Long:  "List the project's secrets: their names, scopes and versions. Never their values.",
	RunE:  runSecretsList,
}

func runSecretsList(cmd *cobra.Command, args []string) error {
	client, err := newClient()
	if err != nil {
		return err
	}

	secrets, err := client.ListSecrets(cmd.Context())
	if err != nil {
		return err
	}

	sort.Slice(secrets, func(i, j int) bool {
		si, sj := getString(secrets[i], "scope"), getString(secrets[j], "scope")
		if si != sj {
			return si < sj
		}
		return getString(secrets[i], "name") < getString(secrets[j], "name")
	})

	if isOutput("json") {
		return outputJSON(secrets)
	}

	if len(secrets) == 0 {
		fmt.Println("No secrets found.")
		return nil
	}

	var rows [][]string
	for _, s := range secrets {
		scope := getString(s, "scope")
		if scope == "" {
			scope = "(all)"
		}
		by := "-"
		if principal, ok := s["updatedBy"].(map[string]any); ok {
			by = fmt.Sprintf("%s %s", getString(principal, "type"), getString(principal, "externalId"))
		}
		rows = append(rows, []string{
			getString(s, "name"),
			scope,
			fmt.Sprintf("%d", int(getFloat64(s, "version"))),
			formatTimestamp(getInt64(s, "updatedAt")),
			by,
		})
	}

	printTable([]string{"Name", "Scope", "Version", "Updated", "By"}, rows)
	return nil
}

// secrets delete

var secretsDeleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a secret",
	Args:  cobra.ExactArgs(1),
	RunE:  runSecretsDelete,
}

func runSecretsDelete(cmd *cobra.Command, args []string) error {
	name := args[0]

	scope, err := secretScope()
	if err != nil {
		return err
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	if err := client.DeleteSecret(cmd.Context(), scope, name); err != nil {
		return err
	}

	fmt.Printf("Deleted secret '%s' for %s.\n", name, describeScope(scope))
	return nil
}
