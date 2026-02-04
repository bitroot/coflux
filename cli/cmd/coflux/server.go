package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/spf13/cobra"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start a local Coflux server",
	Long: `Start a local Coflux server using Docker.

This is a convenience wrapper around Docker (which must be installed and running),
useful for running the server in a development environment.

Examples:
  coflux server
  coflux server --port 8080
  coflux server --data-dir ./my-data`,
	RunE: runServer,
}

var (
	serverPort    int
	serverDataDir string
	serverImage   string
)

func init() {
	serverCmd.Flags().IntVarP(&serverPort, "port", "p", 7777, "Port to run server on")
	serverCmd.Flags().StringVarP(&serverDataDir, "data-dir", "d", "./data", "Directory to store data")
	serverCmd.Flags().StringVar(&serverImage, "image", getDefaultImage(), "Docker image to run")
}

// getDefaultImage returns the default Docker image name
func getDefaultImage() string {
	// Use version if set, otherwise use latest
	if version != "" && version != "dev" {
		return fmt.Sprintf("ghcr.io/cofluxlabs/coflux:%s", version)
	}
	return "ghcr.io/cofluxlabs/coflux:latest"
}

func runServer(cmd *cobra.Command, args []string) error {
	// Resolve data directory to absolute path
	dataDir, err := resolveDataDir(serverDataDir)
	if err != nil {
		return err
	}

	// Determine pull policy based on image name
	pullPolicy := "always"
	if len(serverImage) >= 7 && serverImage[:7] == "sha256:" {
		pullPolicy = "missing"
	}

	// Build docker command
	dockerArgs := []string{
		"run",
		"--pull", pullPolicy,
		"--publish", fmt.Sprintf("%d:7777", serverPort),
		"--volume", fmt.Sprintf("%s:/data", dataDir),
		serverImage,
	}

	fmt.Printf("Starting Coflux server on port %d...\n", serverPort)
	fmt.Printf("Data directory: %s\n", dataDir)

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
