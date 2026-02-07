package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/bitroot/coflux/cli/internal/adapter"
	"github.com/bitroot/coflux/cli/internal/config"
	"github.com/bitroot/coflux/cli/internal/worker"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var workerCmd = &cobra.Command{
	Use:   "worker <modules...>",
	Short: "Start a Coflux worker",
	Long: `Start a worker that connects to the Coflux server, registers discovered
targets, and executes workflows and tasks as assigned.

Examples:
  coflux worker myapp.workflows myapp.tasks
  coflux worker --dev myapp.workflows        # Development mode (watch + register)`,
	Args: cobra.MinimumNArgs(1),
	RunE: runWorker,
}

var (
	workerWatch       bool
	workerRegister    bool
	workerDev         bool
	workerConcurrency int
	workerSession     string
	workerProvides    []string
	workerAdapter     []string
)

func init() {
	workerCmd.Flags().BoolVar(&workerWatch, "watch", false, "Watch for file changes and reload")
	workerCmd.Flags().BoolVar(&workerRegister, "register", false, "Automatically register modules with the server")
	workerCmd.Flags().BoolVar(&workerDev, "dev", false, "Enable development mode (implies --watch and --register)")
	workerCmd.Flags().IntVar(&workerConcurrency, "concurrency", 0, "Number of concurrent executors (default: CPU count + 4)")
	workerCmd.Flags().StringVar(&workerSession, "session", "", "Session ID (for pool-launched workers)")
	workerCmd.Flags().StringSliceVar(&workerProvides, "provides", nil, "Features that this worker provides (e.g., --provides gpu=A100,H100 --provides region=us-east-1)")
	workerCmd.Flags().StringSliceVar(&workerAdapter, "adapter", nil, "Adapter command (e.g., --adapter python,-m,coflux)")
}

func runWorker(cmd *cobra.Command, args []string) error {
	// Override concurrency if specified via flag
	if workerConcurrency > 0 {
		viper.Set("concurrency", workerConcurrency)
	}

	// Load config from viper (merges defaults, config file, env vars, flags)
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	// Override session if specified via flag
	if workerSession != "" {
		cfg.Session = workerSession
	}

	// Merge provides from flag with config
	if len(workerProvides) > 0 {
		if cfg.Provides == nil {
			cfg.Provides = make(map[string][]string)
		}
		for _, p := range workerProvides {
			// Parse "key=value1,value2" format
			parts := strings.SplitN(p, "=", 2)
			if len(parts) == 2 {
				key := parts[0]
				values := strings.Split(parts[1], ",")
				cfg.Provides[key] = values
			} else {
				// If no =, treat as boolean flag "key=true"
				cfg.Provides[p] = []string{"true"}
			}
		}
	}

	// Resolve token (may involve auth flow)
	token, err := resolveToken()
	if err != nil {
		return err
	}
	cfg.Server.Token = token

	modules := args

	// Override adapter if specified via flag
	if len(workerAdapter) > 0 {
		cfg.Adapter = workerAdapter
	}

	// Check adapter is configured
	if len(cfg.Adapter) == 0 {
		return fmt.Errorf("no adapter configured; run 'coflux setup' or add 'adapter' to coflux.toml")
	}

	// Setup logging
	logger := getLogger()

	// Create adapter from config
	cmdAdapter := adapter.NewCommandAdapter(cfg.Adapter)

	// Create worker
	w := worker.New(cfg, cmdAdapter, logger)

	// Setup signal handling
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutting down...")
		cancel()
	}()

	// Determine if we should register manifests
	shouldRegister := workerRegister || workerDev

	// Determine if we should watch for changes
	shouldWatch := workerWatch || workerDev

	if workerDev {
		logger.Info("development mode enabled (watch + register)")
	}

	if shouldWatch {
		return runWorkerWithWatch(ctx, cfg, cmdAdapter, modules, shouldRegister, logger, sigCh)
	}

	// Run worker
	logger.Info("starting worker",
		"workspace", cfg.Workspace,
		"host", cfg.Server.Host,
		"concurrency", cfg.Concurrency,
		"register", shouldRegister,
	)

	if err := w.Run(ctx, modules, shouldRegister); err != nil {
		if ctx.Err() != nil {
			// Normal shutdown
			logger.Info("worker stopped")
			return nil
		}
		return fmt.Errorf("worker error: %w", err)
	}

	return nil
}

// runWorkerWithWatch runs the worker with file watching enabled.
// When Python files change, the worker is restarted.
func runWorkerWithWatch(
	ctx context.Context,
	cfg *config.Config,
	cmdAdapter *adapter.CommandAdapter,
	modules []string,
	shouldRegister bool,
	logger *slog.Logger,
	sigCh chan os.Signal,
) error {
	// Create file watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create file watcher: %w", err)
	}
	defer func() { _ = watcher.Close() }()

	// Watch current directory recursively for Python files
	if err := watchDirectoryRecursive(watcher, ".", logger); err != nil {
		return fmt.Errorf("failed to setup file watching: %w", err)
	}

	logger.Info("watching for file changes", "directory", ".")

	for {
		// Create a cancellable context for this worker run
		runCtx, runCancel := context.WithCancel(ctx)

		// Channel to signal worker completion
		workerDone := make(chan error, 1)

		// Start worker in goroutine
		go func() {
			w := worker.New(cfg, cmdAdapter, logger)
			logger.Info("starting worker",
				"workspace", cfg.Workspace,
				"host", cfg.Server.Host,
				"concurrency", cfg.Concurrency,
				"register", shouldRegister,
			)
			workerDone <- w.Run(runCtx, modules, shouldRegister)
		}()

		// Wait for Python file change, signal, or worker exit
		restart := false
		for !restart {
			select {
			case <-sigCh:
				logger.Info("shutting down...")
				runCancel()
				<-workerDone
				return nil

			case err := <-workerDone:
				runCancel()
				if ctx.Err() != nil {
					logger.Info("worker stopped")
					return nil
				}
				if err != nil {
					return fmt.Errorf("worker error: %w", err)
				}
				return nil

			case event := <-watcher.Events:
				if isPythonFileChange(event) {
					logger.Info("change detected, reloading...", "file", event.Name)
					restart = true
				}
				// For non-Python changes, continue waiting

			case err := <-watcher.Errors:
				logger.Error("file watcher error", "error", err)
			}
		}

		// Restart: cancel current worker and wait for it to stop
		runCancel()
		<-workerDone
		// Small delay to let file writes complete
		time.Sleep(100 * time.Millisecond)
	}
}

// watchDirectoryRecursive adds all directories under root to the watcher
func watchDirectoryRecursive(watcher *fsnotify.Watcher, root string, logger *slog.Logger) error {
	count := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			// Skip hidden directories and common non-source directories
			// But don't skip "." itself (the root)
			name := info.Name()
			if name != "." && (strings.HasPrefix(name, ".") || name == "__pycache__" || name == "node_modules" || name == "venv" || name == ".venv") {
				return filepath.SkipDir
			}
			if err := watcher.Add(path); err != nil {
				logger.Warn("failed to watch directory", "path", path, "error", err)
			} else {
				count++
			}
		}
		return nil
	})
	logger.Debug("directory watch setup complete", "count", count)
	return err
}

// isPythonFileChange checks if the event is a relevant Python file change
func isPythonFileChange(event fsnotify.Event) bool {
	// Only care about write and create events
	if event.Op&(fsnotify.Write|fsnotify.Create) == 0 {
		return false
	}
	// Only care about .py files
	return strings.HasSuffix(event.Name, ".py")
}
