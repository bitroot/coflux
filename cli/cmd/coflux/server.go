package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/bitroot/coflux/cli/internal/version"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start a local Coflux server",
	Long: `Start a local Coflux server using Docker.

This is a convenience wrapper around Docker (which must be installed and running),
useful for running the server in a development environment.

Server options can be set via flags, the [server] section in coflux.toml, or
COFLUX_SERVER_* environment variables.

Examples:
  coflux server
  coflux server --port 8080
  coflux server --data-dir ./my-data
  coflux server --super-token mytoken --no-auth`,
	RunE: runServer,
}

var (
	serverNoAuth         bool
	serverSuperToken     string
	serverSuperTokenHash string
)

func init() {
	serverCmd.Flags().IntP("port", "p", 0, "Port to run server on (default 7777)")
	serverCmd.Flags().StringP("data-dir", "d", "", "Directory to store data (default ./data)")
	serverCmd.Flags().String("image", "", "Docker image to run")
	serverCmd.Flags().String("project", "", "Restrict server to a single project")
	serverCmd.Flags().String("public-host", "", "Public-facing host (use % prefix for subdomain routing)")
	serverCmd.Flags().BoolVar(&serverNoAuth, "no-auth", false, "Disable authentication")
	serverCmd.Flags().StringVar(&serverSuperToken, "super-token", "", "Super token (will be hashed)")
	serverCmd.Flags().StringVar(&serverSuperTokenHash, "super-token-hash", "", "Pre-hashed super token (SHA-256 hex)")
	serverCmd.Flags().String("secret", "", "Server secret for signing service tokens")
	serverCmd.Flags().StringSlice("studio-teams", nil, "Team IDs allowed for Studio auth")
	serverCmd.Flags().StringSlice("launcher-types", nil, "Allowed launcher types (docker, process)")

	serverCmd.MarkFlagsMutuallyExclusive("super-token", "super-token-hash")

	// Bind flags to viper under the server.* namespace
	viper.BindPFlag("server.port", serverCmd.Flags().Lookup("port"))
	viper.BindPFlag("server.data_dir", serverCmd.Flags().Lookup("data-dir"))
	viper.BindPFlag("server.image", serverCmd.Flags().Lookup("image"))
	viper.BindPFlag("server.project", serverCmd.Flags().Lookup("project"))
	viper.BindPFlag("server.public_host", serverCmd.Flags().Lookup("public-host"))
	viper.BindPFlag("server.secret", serverCmd.Flags().Lookup("secret"))
	viper.BindPFlag("server.studio_teams", serverCmd.Flags().Lookup("studio-teams"))
	viper.BindPFlag("server.launcher_types", serverCmd.Flags().Lookup("launcher-types"))
}

// getDefaultImage returns the default Docker image name.
// Uses the API version (e.g., "0.9") as the tag, so patch releases
// are pulled automatically. Users can pin to an exact version with --image.
func getDefaultImage() string {
	apiVersion := version.APIVersion()
	if apiVersion != "dev" && apiVersion != "" {
		return fmt.Sprintf("ghcr.io/cofluxlabs/coflux:%s", apiVersion)
	}
	return "ghcr.io/cofluxlabs/coflux:latest"
}

func runServer(cmd *cobra.Command, args []string) error {
	port := viper.GetInt("server.port")
	if port == 0 {
		port = 7777
	}

	dataDir := viper.GetString("server.data_dir")
	if dataDir == "" {
		dataDir = "./data"
	}

	image := viper.GetString("server.image")
	if image == "" {
		image = getDefaultImage()
	}

	// Resolve data directory to absolute path
	absDataDir, err := resolveDataDir(dataDir)
	if err != nil {
		return err
	}

	// Determine pull policy based on image name
	pullPolicy := "always"
	if len(image) >= 7 && image[:7] == "sha256:" {
		pullPolicy = "missing"
	}

	// Build docker command
	dockerArgs := []string{
		"run",
		"--pull", pullPolicy,
		"--publish", fmt.Sprintf("%d:7777", port),
		"--volume", fmt.Sprintf("%s:/data", absDataDir),
	}

	// Add environment variables for server configuration
	if project := viper.GetString("server.project"); project != "" {
		dockerArgs = append(dockerArgs, "--env", "COFLUX_PROJECT="+project)
	}
	if publicHost := viper.GetString("server.public_host"); publicHost != "" {
		dockerArgs = append(dockerArgs, "--env", "COFLUX_PUBLIC_HOST="+publicHost)
	}
	if serverNoAuth {
		dockerArgs = append(dockerArgs, "--env", "COFLUX_REQUIRE_AUTH=false")
	}

	// Handle super token: flag takes precedence, then config
	tokenHash := serverSuperTokenHash
	if serverSuperToken != "" {
		tokenHash = hashToken(serverSuperToken)
	} else if tokenHash == "" {
		// Check config file values
		if t := viper.GetString("server.super_token"); t != "" {
			tokenHash = hashToken(t)
		} else if h := viper.GetString("server.super_token_hash"); h != "" {
			tokenHash = h
		}
	}
	if tokenHash != "" {
		dockerArgs = append(dockerArgs, "--env", "COFLUX_SUPER_TOKEN_HASH="+tokenHash)
	}

	if secret := viper.GetString("server.secret"); secret != "" {
		dockerArgs = append(dockerArgs, "--env", "COFLUX_SECRET="+secret)
	}
	if teams := viper.GetStringSlice("server.studio_teams"); len(teams) > 0 {
		dockerArgs = append(dockerArgs, "--env", "COFLUX_STUDIO_TEAMS="+strings.Join(teams, ","))
	}
	if types := viper.GetStringSlice("server.launcher_types"); len(types) > 0 {
		dockerArgs = append(dockerArgs, "--env", "COFLUX_LAUNCHER_TYPES="+strings.Join(types, ","))
	}

	// Check config-level auth setting (--no-auth flag handled above)
	if !serverNoAuth {
		if auth := viper.Get("server.auth"); auth != nil {
			if authBool, ok := auth.(bool); ok && !authBool {
				dockerArgs = append(dockerArgs, "--env", "COFLUX_REQUIRE_AUTH=false")
			}
		}
	}

	dockerArgs = append(dockerArgs, image)

	fmt.Printf("Starting Coflux server on port %d...\n", port)
	fmt.Printf("Data directory: %s\n", absDataDir)

	// Run docker
	dockerCmd := exec.Command("docker", dockerArgs...)
	dockerCmd.Stdout = os.Stdout
	dockerCmd.Stderr = os.Stderr
	dockerCmd.Stdin = os.Stdin

	if err := dockerCmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode())
		}
		return fmt.Errorf("failed to run docker: %w", err)
	}

	return nil
}

// hashToken returns the SHA-256 hex digest of a token
func hashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// resolveDataDir resolves and creates the data directory
func resolveDataDir(dir string) (string, error) {
	// Get absolute path
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("failed to resolve data directory: %w", err)
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(absDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create data directory: %w", err)
	}

	return absDir, nil
}
