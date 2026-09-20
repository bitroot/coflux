package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

var secretsCmd = &cobra.Command{
	Use:   "secrets",
	Short: "Manage secrets",
	Long: `Manage secrets: values that pools refer to by name - a launcher's credentials,
or environment variables for workers - and that never appear in a pool's
configuration.

Every secret is set for one or more workspaces, given as --workspaces: a
workspace name for that one alone, 'development/*' for those under it, or '*'
for all of them. The '*' spans any depth, so 'development/*' reaches
'development/joe/feature-1' too, but it doesn't include 'development' itself -
give both ('development,development/*') for that.

Where scopes overlap the nearest wins: an exact workspace beats a longer
prefix, which beats a shorter one, which beats '*'.`,
}

var (
	secretsWorkspaces string
	secretsFromEnv    string
	secretsFromFile   string
)

func init() {
	for _, cmd := range []*cobra.Command{secretsSetCmd, secretsDeleteCmd} {
		cmd.Flags().StringVar(&secretsWorkspaces, "workspaces", "", "Comma-separated workspace patterns the secret applies to")
		cmd.MarkFlagRequired("workspaces")
	}
	secretsSetCmd.Flags().StringVar(&secretsFromEnv, "from-env", "", "Read the value from this environment variable")
	secretsSetCmd.Flags().StringVar(&secretsFromFile, "from-file", "", "Read the value from this file")
	secretsCmd.AddCommand(secretsSetCmd, secretsListCmd, secretsDeleteCmd)
}

// secretWorkspaces is the patterns a secret is set for or deleted from. The
// secret is stored once per pattern, so each is listed and rotated on its own.
func secretWorkspaces() ([]string, error) {
	var workspaces []string
	for _, part := range strings.Split(secretsWorkspaces, ",") {
		if part = strings.TrimSpace(part); part != "" {
			workspaces = append(workspaces, part)
		}
	}
	if len(workspaces) == 0 {
		return nil, fmt.Errorf("--workspaces can't be empty")
	}
	return workspaces, nil
}

func describeWorkspaces(workspaces []string) string {
	quoted := make([]string, len(workspaces))
	for i, workspace := range workspaces {
		quoted[i] = fmt.Sprintf("'%s'", workspace)
	}
	return strings.Join(quoted, ", ")
}

// secrets set

var secretsSetCmd = &cobra.Command{
	Use:   "set <name>",
	Short: "Set a secret's value",
	Long: `Set a secret's value, creating it or replacing what it had.

The value is read from stdin, so it never appears on the command line or in
shell history:

  printf '%s' "$API_KEY" | coflux secrets set api-key --workspaces '*'
  aws configure export-credentials --profile sandbox |
    coflux secrets set aws-sandbox --workspaces 'production/*'

Or from an environment variable (--from-env) or a file (--from-file). A single
trailing newline is dropped, so 'echo' works too.`,
	Args: cobra.ExactArgs(1),
	RunE: runSecretsSet,
}

func runSecretsSet(cmd *cobra.Command, args []string) error {
	name := args[0]

	workspaces, err := secretWorkspaces()
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

	result, err := client.SetSecret(cmd.Context(), workspaces, name, value)
	if err != nil {
		return err
	}

	for _, secret := range result.Secrets {
		fmt.Printf("Set secret '%s' (version %d) for '%s'.\n", result.Name, secret.Version, secret.Workspaces)
	}
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
	Long:  "List the project's secrets: their names, workspaces and versions. Never their values.",
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
		by := "-"
		if principal, ok := s["updatedBy"].(map[string]any); ok {
			by = fmt.Sprintf("%s %s", getString(principal, "type"), getString(principal, "externalId"))
		}
		rows = append(rows, []string{
			getString(s, "name"),
			getString(s, "scope"),
			fmt.Sprintf("%d", int(getFloat64(s, "version"))),
			formatTimestamp(getInt64(s, "updatedAt")),
			by,
		})
	}

	printTable([]string{"Name", "Workspaces", "Version", "Updated", "By"}, rows)
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

	workspaces, err := secretWorkspaces()
	if err != nil {
		return err
	}

	client, err := newClient()
	if err != nil {
		return err
	}

	result, err := client.DeleteSecret(cmd.Context(), workspaces, name)
	if err != nil {
		return err
	}

	fmt.Printf("Deleted secret '%s' for %s.\n", name, describeWorkspaces(result.Workspaces))
	return nil
}
