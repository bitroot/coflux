package main

import (
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"

	"github.com/bitroot/coflux/cli/internal/config"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	version = "dev"
	cfgFile string
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "coflux",
	Short: "CLI for the Coflux workflow engine",
	Long: `This is a CLI for the Coflux workflow engine.

It supports managing and interacting with the orchestration server, hosting
workers, and authenticating with Studio.`,
	Version:       version,
	SilenceErrors: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Silence usage after arg validation passes — arg/flag errors
		// still show usage, but runtime errors (API failures, etc.) don't.
		cmd.SilenceUsage = true
		return initConfig(cmd, args)
	},
}

func init() {
	// Set defaults (before config file is read)
	// Priority: defaults < config file < env vars < flags
	viper.SetDefault("server.host", "localhost:7777")
	viper.SetDefault("workspace", "default")
	viper.SetDefault("worker.concurrency", min(runtime.NumCPU()+4, 32))
	viper.SetDefault("blobs.threshold", 100)
	viper.SetDefault("logs.batch_size", 100)
	viper.SetDefault("logs.flush_interval", 0.5)
	viper.SetDefault("log_level", "info")

	// Global flags
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "coflux.toml", "Path to configuration file")
	rootCmd.PersistentFlags().StringP("host", "", "", "Server host")
	rootCmd.PersistentFlags().String("token", "", "Authentication token")
	rootCmd.PersistentFlags().StringP("team", "t", "", "Team ID for Studio authentication")
	rootCmd.PersistentFlags().StringP("workspace", "w", "", "Workspace name")
	rootCmd.PersistentFlags().String("log-level", "", "Log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringP("output", "o", "", "Output format (json)")

	// Bind flags to viper
	viper.BindPFlag("server.host", rootCmd.PersistentFlags().Lookup("host"))
	viper.BindPFlag("server.token", rootCmd.PersistentFlags().Lookup("token"))
	viper.BindPFlag("team", rootCmd.PersistentFlags().Lookup("team"))
	viper.BindPFlag("workspace", rootCmd.PersistentFlags().Lookup("workspace"))
	viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	viper.BindPFlag("output", rootCmd.PersistentFlags().Lookup("output"))

	// Command groups
	rootCmd.AddGroup(
		&cobra.Group{ID: "core", Title: "Core Commands:"},
		&cobra.Group{ID: "auth", Title: "Auth Commands:"},
		&cobra.Group{ID: "management", Title: "Management Commands:"},
	)

	// Core commands
	workerCmd.GroupID = "core"
	submitCmd.GroupID = "core"
	runsCmd.GroupID = "core"
	setupCmd.GroupID = "core"
	serverCmd.GroupID = "core"
	rootCmd.AddCommand(workerCmd, submitCmd, runsCmd, setupCmd, serverCmd)

	// Auth commands
	loginCmd.GroupID = "auth"
	logoutCmd.GroupID = "auth"
	rootCmd.AddCommand(loginCmd, logoutCmd)

	// Management commands
	workspacesCmd.GroupID = "management"
	manifestsCmd.GroupID = "management"
	poolsCmd.GroupID = "management"
	tokensCmd.GroupID = "management"
	assetsCmd.GroupID = "management"
	blobsCmd.GroupID = "management"
	logsCmd.GroupID = "management"
	sessionsCmd.GroupID = "management"
	queueCmd.GroupID = "management"
	rootCmd.AddCommand(workspacesCmd, manifestsCmd, poolsCmd, tokensCmd, assetsCmd, blobsCmd, logsCmd, sessionsCmd, queueCmd)
}

func initConfig(cmd *cobra.Command, args []string) error {
	// Set up environment variable binding
	viper.SetEnvPrefix("COFLUX")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// Read config file (env var overrides default, flag overrides both)
	configPath := cfgFile
	if envConfig := os.Getenv("COFLUX_CONFIG"); envConfig != "" && !cmd.Flags().Changed("config") {
		configPath = envConfig
	}
	viper.SetConfigFile(configPath)
	viper.SetConfigType("toml")

	if err := viper.ReadInConfig(); err != nil {
		// Config file not found is OK, other errors are not
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			// Only warn if the file exists but can't be read
			if _, statErr := os.Stat(cfgFile); statErr == nil {
				fmt.Fprintf(os.Stderr, "Warning: error reading config file: %v\n", err)
			}
		}
	}

	return nil
}

// loadConfig unmarshals viper config into a Config struct
func loadConfig() (*config.Config, error) {
	cfg := &config.Config{}
	if err := viper.Unmarshal(cfg, func(dc *mapstructure.DecoderConfig) {
		dc.ErrorUnused = true
	}); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Handle secure flag default (depends on host value)
	if cfg.Server.Secure == nil {
		secure := !config.IsLocalhost(cfg.Server.Host)
		cfg.Server.Secure = &secure
	}

	return cfg, nil
}

// getHost returns the resolved host with secure detection
func getHost() string {
	return viper.GetString("server.host")
}

// getWorkspace returns the resolved workspace
func getWorkspace() string {
	return viper.GetString("workspace")
}

// getTeam returns the resolved team
func getTeam() string {
	return viper.GetString("team")
}

// getToken returns the resolved token
func getToken() string {
	return viper.GetString("server.token")
}

// isSecure determines if HTTPS should be used
func isSecure() bool {
	if viper.IsSet("server.secure") {
		return viper.GetBool("server.secure")
	}
	return !config.IsLocalhost(getHost())
}

// getLogger creates a logger with the configured log level
func getLogger() *slog.Logger {
	levelStr := viper.GetString("log_level")
	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelWarn
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	}))
}
