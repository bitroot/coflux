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
	Use:   "worker [modules...]",
	Short: "Start a Coflux worker",
	Long: `Start a worker that connects to the Coflux server, registers discovered
targets, and executes workflows and tasks as assigned.

Modules can be specified as arguments or via 'worker.modules' in coflux.toml.
Packages are scanned recursively for targets.

Examples:
  coflux worker myapp
  coflux worker myapp.workflows myapp.tasks
  coflux worker --dev myapp
  coflux worker`,
	RunE: runWorker,
}

var (
	workerWatch        bool
	workerRegister     bool
	workerDev          bool
	workerConcurrency  int
	workerSession      string
	workerProvides     []string
	workerAccepts      []string
	workerAdapter      []string
	workerDrainTimeout time.Duration
)

func init() {
	workerCmd.Flags().BoolVar(&workerWatch, "watch", false, "Watch for file changes and reload")
	workerCmd.Flags().BoolVar(&workerRegister, "register", false, "Automatically register modules with the server")
	workerCmd.Flags().BoolVar(&workerDev, "dev", false, "Enable development mode (implies --watch and --register)")
	workerCmd.Flags().IntVar(&workerConcurrency, "concurrency", 0, "Number of concurrent executors (default: CPU count + 4)")
	workerCmd.Flags().StringVar(&workerSession, "session", "", "Session ID (for pool-launched workers)")
	workerCmd.Flags().StringSliceVar(&workerProvides, "provides", nil, "Features that this worker provides (e.g., --provides gpu:A100,gpu:H100,region:eu)")
	workerCmd.Flags().StringSliceVar(&workerAccepts, "accepts", nil, "Tags that executions must have to be scheduled on this worker (e.g., --accepts priority:high)")
	workerCmd.Flags().StringSliceVar(&workerAdapter, "adapter", nil, "Adapter command (e.g., --adapter python,-m,coflux)")
	workerCmd.Flags().DurationVar(&workerDrainTimeout, "drain-timeout", 2*time.Minute, "How long to wait for in-flight executions to finish before aborting them on shutdown or reload. 0 means wait indefinitely. Signal again to abort the drain early.")
}

func runWorker(cmd *cobra.Command, args []string) error {
	// Override concurrency if specified via flag
	if workerConcurrency > 0 {
		viper.Set("worker.concurrency", workerConcurrency)
	}

	// Load config from viper (merges defaults, config file, env vars, flags)
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	// Resolve session: flag overrides env var (read via viper)
	session := viper.GetString("session")
	if workerSession != "" {
		session = workerSession
	}

	// Append provides from flag to config
	if len(workerProvides) > 0 {
		cfg.Worker.Provides = append(cfg.Worker.Provides, workerProvides...)
	}

	// Append accepts from flag to config
	if len(workerAccepts) > 0 {
		cfg.Worker.Accepts = append(cfg.Worker.Accepts, workerAccepts...)
	}

	// Resolve token (may involve auth flow)
	token, err := resolveToken()
	if err != nil {
		return err
	}
	// For pool-launched workers, the session token doubles as the auth token
	if token == "" && session != "" {
		token = session
	}
	cfg.Token = token

	// Resolve modules: CLI args override config
	modules, err := resolveModules(args, cfg)
	if err != nil {
		return err
	}

	// Override adapter if specified via flag
	if len(workerAdapter) > 0 {
		cfg.Worker.Adapter = workerAdapter
	}

	// Setup logging
	logger := getLogger()

	// Auto-detect adapter if not configured
	if len(cfg.Worker.Adapter) == 0 {
		detections := detectAdapters()
		if len(detections) == 0 {
			return fmt.Errorf("no adapter configured; run 'coflux setup' or add 'worker.adapter' to coflux.toml")
		}
		best := detections[0]
		for _, d := range detections[1:] {
			if d.Confidence > best.Confidence {
				best = d
			}
		}
		cfg.Worker.Adapter = best.Command
		logger.Info("auto-detected adapter", "name", best.Name, "command", strings.Join(best.Command, " "))
	}

	// Create adapter from config
	cmdAdapter := adapter.NewCommandAdapter(cfg.Worker.Adapter)

	// Create worker
	w := worker.New(cfg, cmdAdapter, session, logger)

	// Setup signal handling with three phases:
	//   1st signal: close shutdownCh — triggers a graceful drain
	//   2nd signal: close drainAbortCh — aborts the drain early (kill in-flight)
	//   3rd signal: os.Exit — last resort if shutdown itself is stuck
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	shutdownCh := make(chan struct{})
	drainAbortCh := make(chan struct{})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutting down (signal again to abort drain early)")
		close(shutdownCh)
		<-sigCh
		logger.Warn("aborting drain")
		close(drainAbortCh)
		<-sigCh
		logger.Error("forced shutdown")
		os.Exit(1)
	}()

	// Determine if we should register manifests
	shouldRegister := workerRegister || workerDev

	// Determine if we should watch for changes
	shouldWatch := workerWatch || workerDev

	if shouldWatch {
		return runWorkerWithWatch(ctx, cfg, cmdAdapter, session, modules, shouldRegister, shutdownCh, drainAbortCh, logger)
	}

	// Run worker
	logger.Info("starting worker",
		"workspace", cfg.Workspace,
		"host", cfg.Host,
		"modules", modules,
		"concurrency", cfg.Worker.Concurrency,
		"register", shouldRegister,
	)

	workerDone := make(chan error, 1)
	go func() {
		workerDone <- w.Run(ctx, modules, shouldRegister)
	}()

	select {
	case <-shutdownCh:
		drainWorker(w, workerDrainTimeout, drainAbortCh, logger)
		cancel()
		<-workerDone
		logger.Info("worker stopped")
		return nil
	case err := <-workerDone:
		if err != nil {
			return fmt.Errorf("worker error: %w", err)
		}
		return nil
	}
}

// drainWorker runs a graceful drain with the configured timeout, aborting
// early if abortCh is closed.
func drainWorker(w *worker.Worker, timeout time.Duration, abortCh <-chan struct{}, logger *slog.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-abortCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	if timeout > 0 {
		logger.Info("draining in-flight executions", "timeout", timeout)
	} else {
		logger.Info("draining in-flight executions (no timeout; signal again to abort)")
	}
	if remaining := w.Drain(ctx, timeout); remaining > 0 {
		logger.Warn("drain incomplete; aborting executions", "remaining", remaining)
	} else {
		logger.Info("drain complete")
	}
}

// runWorkerWithWatch runs the worker with file watching enabled.
// When Python files change, the worker is restarted (with a graceful
// drain in between). Closing shutdownCh triggers the same drain-then-stop
// path; closing drainAbortCh aborts any in-flight drain early.
func runWorkerWithWatch(
	ctx context.Context,
	cfg *config.Config,
	cmdAdapter *adapter.CommandAdapter,
	session string,
	modules []string,
	shouldRegister bool,
	shutdownCh <-chan struct{},
	drainAbortCh <-chan struct{},
	logger *slog.Logger,
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
		// Re-resolve token before each run (project tokens may have expired)
		token, err := resolveToken()
		if err != nil {
			return fmt.Errorf("failed to refresh token: %w", err)
		}
		cfg.Token = token

		// Create a cancellable context for this worker run
		runCtx, runCancel := context.WithCancel(ctx)

		// Channel to signal worker completion
		workerDone := make(chan error, 1)

		// Start worker in goroutine
		w := worker.New(cfg, cmdAdapter, session, logger)
		go func() {
			logger.Info("starting worker",
				"workspace", cfg.Workspace,
				"host", cfg.Host,
				"concurrency", cfg.Worker.Concurrency,
				"register", shouldRegister,
			)
			workerDone <- w.Run(runCtx, modules, shouldRegister)
		}()

		// Wait for a file change, shutdown, or worker exit.
		reason := ""
		for reason == "" {
			select {
			case <-shutdownCh:
				reason = "shutdown"

			case err := <-workerDone:
				runCancel()
				if err != nil {
					return fmt.Errorf("worker error: %w", err)
				}
				return nil

			case event := <-watcher.Events:
				if isPythonFileChange(event) {
					logger.Info("change detected, reloading...", "file", event.Name)
					reason = "reload"
				}
				// For non-Python changes, continue waiting

			case err := <-watcher.Errors:
				logger.Error("file watcher error", "error", err)
			}
		}

		// Drain in-flight executions (keeps the WebSocket open so results
		// can still be reported), then tear down.
		drainWorker(w, workerDrainTimeout, drainAbortCh, logger)
		runCancel()
		<-workerDone

		if reason == "shutdown" {
			logger.Info("worker stopped")
			return nil
		}

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
