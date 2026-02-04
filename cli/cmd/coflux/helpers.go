package main

import (
	"fmt"
	"os"
	"time"

	"github.com/bitroot/coflux/cli/internal/api"
	"github.com/bitroot/coflux/cli/internal/auth"
)

const studioURL = "https://studio.coflux.com"

// resolveToken resolves the authentication token (either direct or via Studio)
func resolveToken() (string, error) {
	token := getToken()
	if token != "" {
		return token, nil
	}

	team := getTeam()
	if team != "" {
		url := os.Getenv("COFLUX_STUDIO_URL")
		if url == "" {
			url = studioURL
		}
		return auth.GetProjectToken(team, getHost(), url, 60)
	}

	return "", nil
}

// newClient creates a new API client with resolved config
func newClient() (*api.Client, error) {
	token, err := resolveToken()
	if err != nil {
		return nil, err
	}
	return api.NewClient(getHost(), isSecure(), token), nil
}

// requireWorkspace returns the workspace or an error if not set
func requireWorkspace() (string, error) {
	ws := getWorkspace()
	if ws == "" {
		return "", fmt.Errorf("workspace is required (use --workspace, COFLUX_WORKSPACE, or set in config)")
	}
	return ws, nil
}

// splitTarget splits a full target name into module and name
func splitTarget(fullName string) (module, name string) {
	for i := len(fullName) - 1; i >= 0; i-- {
		if fullName[i] == '.' {
			return fullName[:i], fullName[i+1:]
		}
	}
	return "", fullName
}

// printTable prints a formatted table
func printTable(headers []string, rows [][]string) {
	if len(rows) == 0 {
		return
	}

	// Calculate column widths
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	// Print headers
	for i, h := range headers {
		fmt.Printf("%-*s  ", widths[i], h)
	}
	fmt.Println()

	// Print rows
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) {
				fmt.Printf("%-*s  ", widths[i], cell)
			}
		}
		fmt.Println()
	}
}

// formatTimestamp formats a Unix timestamp
func formatTimestamp(ts int64) string {
	if ts == 0 {
		return ""
	}
	t := time.Unix(ts, 0).UTC()
	return t.Format("2006-01-02 15:04:05 UTC")
}

// humanSize formats bytes as human-readable size
func humanSize(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d bytes", bytes)
	}
	value := float64(bytes) / 1024
	for _, unit := range []string{"KiB", "MiB", "GiB"} {
		if value < 1024 {
			return fmt.Sprintf("%.1f%s", value, unit)
		}
		value /= 1024
	}
	return fmt.Sprintf("%.1fTiB", value)
}
