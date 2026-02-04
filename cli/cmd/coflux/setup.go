package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"
)

var (
	setupHost      string
	setupWorkspace string
	setupModules   string
	setupAdapter   string
	setupDetect    bool
)

var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Set up coflux.toml for this project",
	Long: `Sets up the coflux.toml configuration file for this project.

This command helps configure your project to work with Coflux by:
- Setting the server host and workspace
- Detecting your Python environment (venv, poetry, uv)
- Configuring which modules contain your tasks/workflows

Examples:
  coflux setup                    # Interactive setup
  coflux setup --detect           # Auto-detect adapter
  coflux setup --workspace myapp  # Non-interactive with flags`,
	RunE: runSetup,
}

func init() {
	setupCmd.Flags().StringVarP(&setupHost, "host", "H", "", "Server host")
	setupCmd.Flags().StringVarP(&setupWorkspace, "workspace", "w", "", "Workspace name")
	setupCmd.Flags().StringVarP(&setupModules, "modules", "m", "", "Modules containing tasks/workflows (comma-separated)")
	setupCmd.Flags().StringVar(&setupAdapter, "adapter", "", "Adapter command (e.g., 'python -m coflux')")
	setupCmd.Flags().BoolVar(&setupDetect, "detect", false, "Auto-detect adapter without prompting")
}

// AdapterDetection represents a detected adapter environment
type AdapterDetection struct {
	Name       string   // Human-readable name (e.g., "Python (.venv/)")
	Command    []string // Command to use
	Confidence int      // Higher = more confident this is the right choice
}

// detectAdapters looks for common Python environments and returns detected options
func detectAdapters() []AdapterDetection {
	var detections []AdapterDetection

	// Check for .venv directory (highest priority - explicit virtual env)
	if _, err := os.Stat(".venv/bin/python"); err == nil {
		detections = append(detections, AdapterDetection{
			Name:       "Python (.venv/)",
			Command:    []string{".venv/bin/python", "-m", "coflux"},
			Confidence: 100,
		})
	}

	// Check for venv directory
	if _, err := os.Stat("venv/bin/python"); err == nil {
		detections = append(detections, AdapterDetection{
			Name:       "Python (venv/)",
			Command:    []string{"venv/bin/python", "-m", "coflux"},
			Confidence: 90,
		})
	}

	// Check for poetry
	if _, err := os.Stat("poetry.lock"); err == nil {
		detections = append(detections, AdapterDetection{
			Name:       "Python (poetry)",
			Command:    []string{"poetry", "run", "python", "-m", "coflux"},
			Confidence: 80,
		})
	}

	// Check for uv
	if _, err := os.Stat("uv.lock"); err == nil {
		detections = append(detections, AdapterDetection{
			Name:       "Python (uv)",
			Command:    []string{"uv", "run", "python", "-m", "coflux"},
			Confidence: 80,
		})
	}

	return detections
}

// formatCommand formats a command slice as a TOML-friendly string
func formatCommand(cmd []string) string {
	parts := make([]string, len(cmd))
	for i, part := range cmd {
		parts[i] = fmt.Sprintf("%q", part)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// parseCommand parses a space-separated command string into a slice
func parseCommand(s string) []string {
	// Simple space-based split (doesn't handle quoted strings)
	return strings.Fields(s)
}

func runSetup(cmd *cobra.Command, args []string) error {
	configPath := "coflux.toml"
	reader := bufio.NewReader(os.Stdin)

	// Load existing config if present
	existingConfig := make(map[string]any)
	if data, err := os.ReadFile(configPath); err == nil {
		toml.Decode(string(data), &existingConfig)
	}

	// Get existing values for defaults
	existingHost := "localhost:7777"
	if server, ok := existingConfig["server"].(map[string]any); ok {
		if h, ok := server["host"].(string); ok && h != "" {
			existingHost = h
		}
	}
	existingWorkspace := filepath.Base(mustGetwd())
	if w, ok := existingConfig["workspace"].(string); ok && w != "" {
		existingWorkspace = w
	}
	var existingModules []string
	if m, ok := existingConfig["modules"].([]any); ok {
		for _, v := range m {
			if s, ok := v.(string); ok {
				existingModules = append(existingModules, s)
			}
		}
	}
	var existingAdapter []string
	if a, ok := existingConfig["adapter"].([]any); ok {
		for _, v := range a {
			if s, ok := v.(string); ok {
				existingAdapter = append(existingAdapter, s)
			}
		}
	}

	fmt.Println("=== Server ===")

	// Get host
	host := setupHost
	if host == "" {
		fmt.Printf("Host [%s]: ", existingHost)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "" {
			host = existingHost
		} else {
			host = input
		}
	}

	// Get workspace
	workspace := setupWorkspace
	if workspace == "" {
		fmt.Printf("Workspace [%s]: ", existingWorkspace)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "" {
			workspace = existingWorkspace
		} else {
			workspace = input
		}
	}

	fmt.Println()
	fmt.Println("=== Adapter ===")

	// Detect or configure adapter
	var adapterCmd []string
	if setupAdapter != "" {
		// Explicitly provided via flag
		adapterCmd = parseCommand(setupAdapter)
	} else if len(existingAdapter) > 0 && !setupDetect {
		// Use existing config
		adapterCmd = existingAdapter
		fmt.Printf("Using existing adapter: %s\n", formatCommand(adapterCmd))
	} else {
		// Try to detect
		detections := detectAdapters()
		if len(detections) > 0 {
			// Use highest confidence detection
			best := detections[0]
			for _, d := range detections[1:] {
				if d.Confidence > best.Confidence {
					best = d
				}
			}

			if setupDetect {
				// Auto-detect mode - use best match without prompting
				adapterCmd = best.Command
				fmt.Printf("Detected: %s\n", best.Name)
			} else {
				// Interactive - confirm with user
				fmt.Printf("Detected: %s\n", best.Name)
				fmt.Printf("  adapter = %s\n", formatCommand(best.Command))
				fmt.Print("Use this? [Y/n]: ")
				input, _ := reader.ReadString('\n')
				input = strings.TrimSpace(strings.ToLower(input))
				if input == "" || input == "y" || input == "yes" {
					adapterCmd = best.Command
				} else {
					// Let user enter manually
					fmt.Print("Enter adapter command: ")
					input, _ := reader.ReadString('\n')
					input = strings.TrimSpace(input)
					if input != "" {
						adapterCmd = parseCommand(input)
					}
				}
			}
		} else {
			// Nothing detected
			fmt.Println("No Python environment detected.")
			fmt.Println("Examples:")
			fmt.Println("  .venv/bin/python -m coflux")
			fmt.Println("  poetry run python -m coflux")
			fmt.Println("  uv run python -m coflux")
			fmt.Print("Enter adapter command: ")
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			if input != "" {
				adapterCmd = parseCommand(input)
			}
		}
	}

	if len(adapterCmd) == 0 {
		fmt.Println("Warning: No adapter configured. You'll need to add 'adapter' to coflux.toml before running 'coflux worker'.")
	}

	fmt.Println()
	fmt.Println("=== Modules ===")

	// Get modules
	var modules []string
	if setupModules != "" {
		// Explicitly provided via flag
		for _, m := range strings.Split(setupModules, ",") {
			m = strings.TrimSpace(m)
			if m != "" {
				modules = append(modules, m)
			}
		}
	} else if len(existingModules) > 0 {
		// Use existing config
		modules = existingModules
		fmt.Printf("Using existing modules: %s\n", strings.Join(modules, ", "))
	} else {
		// Prompt user
		fmt.Println("Which Python modules contain your tasks/workflows?")
		fmt.Print("Modules (comma-separated): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		for _, m := range strings.Split(input, ",") {
			m = strings.TrimSpace(m)
			if m != "" {
				modules = append(modules, m)
			}
		}
	}

	if len(modules) == 0 {
		fmt.Println("Warning: No modules configured. You'll need to add 'modules' to coflux.toml or pass them to 'coflux worker'.")
	}

	// Update config
	if existingConfig["server"] == nil {
		existingConfig["server"] = make(map[string]any)
	}
	existingConfig["server"].(map[string]any)["host"] = host
	existingConfig["workspace"] = workspace
	if len(adapterCmd) > 0 {
		existingConfig["adapter"] = adapterCmd
	}
	if len(modules) > 0 {
		existingConfig["modules"] = modules
	}

	// Write config
	fmt.Println()
	fmt.Println("Writing configuration...")

	file, err := os.Create(configPath)
	if err != nil {
		return fmt.Errorf("failed to create config file: %w", err)
	}

	encoder := toml.NewEncoder(file)
	if err := encoder.Encode(existingConfig); err != nil {
		_ = file.Close()
		return fmt.Errorf("failed to write config: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close config file: %w", err)
	}

	absPath, _ := filepath.Abs(configPath)
	fmt.Printf("Configuration written to '%s'.\n", absPath)
	fmt.Println()
	fmt.Println("Run 'coflux worker' to start.")
	return nil
}

func mustGetwd() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}
