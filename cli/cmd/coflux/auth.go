package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"

	"github.com/bitroot/coflux/cli/internal/auth"
	"github.com/spf13/cobra"
)

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Authenticate with Coflux Studio",
	Long: `Authenticate with Coflux Studio using the device authorization flow.

This will open a browser to complete authentication and store the
credentials locally for future use.`,
	RunE: runLogin,
}

var logoutCmd = &cobra.Command{
	Use:   "logout",
	Short: "Remove stored authentication credentials",
	Long:  `Remove locally stored authentication credentials for Coflux Studio.`,
	RunE:  runLogout,
}

var (
	loginNoBrowser bool
)

func init() {
	loginCmd.Flags().BoolVar(&loginNoBrowser, "no-browser", false, "Don't open a browser automatically")
}

func runLogin(cmd *cobra.Command, args []string) error {
	fmt.Println("Starting authentication with Coflux Studio...")

	// Resolve Studio URL
	url := os.Getenv("COFLUX_STUDIO_URL")
	if url == "" {
		url = studioURL
	}

	// Get hostname for device name
	hostname, _ := os.Hostname()

	// Start device flow
	start, err := auth.StartDeviceFlow(url, hostname)
	if err != nil {
		return fmt.Errorf("failed to start authentication: %w", err)
	}

	// Show verification URL to user
	fmt.Printf("\nPlease open this URL in your browser to authenticate:\n\n")
	fmt.Printf("  %s\n\n", start.VerificationURI)

	// Try to open browser automatically
	if !loginNoBrowser {
		if openErr := openBrowser(start.VerificationURI); openErr != nil {
			fmt.Println("(Unable to open browser automatically)")
		}
	}

	fmt.Println("Waiting for authentication...")

	// Poll for token
	interval := max(start.Interval, 5)
	result, err := auth.PollForToken(start.DeviceCode, url, interval, start.ExpiresIn)
	if err != nil {
		return fmt.Errorf("authentication failed: %w", err)
	}

	// Save credentials
	creds := &auth.Credentials{
		StudioToken: result.AccessToken,
		UserEmail:   result.UserEmail,
		UserID:      result.UserID,
	}
	if err := auth.SaveCredentials(creds); err != nil {
		return fmt.Errorf("failed to save credentials: %w", err)
	}
	_ = auth.ClearTokenCache()

	fmt.Printf("\nAuthentication successful!")
	if result.UserEmail != "" {
		fmt.Printf(" Logged in as %s", result.UserEmail)
	}
	fmt.Println()

	return nil
}

func runLogout(cmd *cobra.Command, args []string) error {
	if err := auth.ClearCredentials(); err != nil {
		return fmt.Errorf("failed to clear credentials: %w", err)
	}

	fmt.Println("Logged out successfully.")
	return nil
}

// openBrowser opens a URL in the default browser
func openBrowser(url string) error {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "linux":
		cmd = exec.Command("xdg-open", url)
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("cmd", "/c", "start", url)
	default:
		return fmt.Errorf("unsupported platform")
	}

	return cmd.Start()
}
