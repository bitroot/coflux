package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/bitroot/coflux/cli/internal/adapter"
)

// ExecutionHandler is called when an executor needs to interact with the server
type ExecutionHandler interface {
	// SubmitExecution submits a child execution
	SubmitExecution(ctx context.Context, params *adapter.SubmitExecutionParams) (map[string]any, error)
	// ResolveReference resolves a reference to get its result
	ResolveReference(ctx context.Context, executionID string, targetExecutionID string, timeoutMs *int64) (*adapter.ResolveResult, error)
	// PersistAsset persists files as an asset
	PersistAsset(ctx context.Context, executionID string, paths []string, metadata map[string]any, preResolved map[string][]any) (map[string]any, error)
	// GetAsset retrieves asset entries
	GetAsset(ctx context.Context, executionID string, assetID string) (map[string]any, error)
	// DownloadBlob downloads a blob to a local file
	DownloadBlob(ctx context.Context, executionID, blobKey, targetPath string) error
	// UploadBlob uploads a local file as a blob
	UploadBlob(ctx context.Context, executionID, sourcePath string) (string, error)
	// Suspend suspends an execution
	Suspend(ctx context.Context, executionID string, executeAfter *int64) error
	// CancelExecution cancels another execution
	CancelExecution(ctx context.Context, executionID string, targetExecutionID string) error
	// RegisterGroup registers a group for organizing child executions
	RegisterGroup(ctx context.Context, executionID string, groupID int, name *string) error
	// RecordLog records a log message (level: 0=debug, 1=stdout, 2=info, 3=stderr, 4=warning, 5=error)
	// Template is the message template, values contains serialized values (each is ["raw", data, refs] or ["blob", key, size, refs])
	RecordLog(ctx context.Context, executionID string, level int, template *string, values map[string]*adapter.Value) error
	// ReportResult reports execution completion
	ReportResult(ctx context.Context, executionID string, result *adapter.Value) error
	// ReportError reports execution failure
	ReportError(ctx context.Context, executionID string, errorType, message, traceback string) error
	// NotifyTerminated notifies the server that an execution's process has exited
	NotifyTerminated(ctx context.Context, executionID string) error
}

// Pool manages executor processes. Each executor handles one execution then
// exits. The pool maintains warm (pre-spawned) executors as an optimisation
// and falls back to on-demand spawning when none are available.
type Pool struct {
	adapter     adapter.Adapter
	concurrency int
	warmTarget  int
	handler     ExecutionHandler
	logger      *slog.Logger

	mu       sync.Mutex
	warm     []*adapter.Executor          // idle, ready executors
	busy     map[string]*adapter.Executor // executionID -> running executor
	aborted  map[string]bool              // executions aborted by the server
	shutdown bool
	cancel   context.CancelFunc
	ctx      context.Context
	wg       sync.WaitGroup // tracks runExecution goroutines
}

// NewPool creates a new executor pool.
// concurrency is the maximum number of concurrent executions.
// warmTarget is the number of warm executors to try to maintain.
func NewPool(adp adapter.Adapter, concurrency int, handler ExecutionHandler, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	warmTarget := concurrency
	if warmTarget > 4 {
		warmTarget = 4
	}
	return &Pool{
		adapter:     adp,
		concurrency: concurrency,
		warmTarget:  warmTarget,
		handler:     handler,
		logger:      logger,
		busy:        make(map[string]*adapter.Executor),
		aborted:     make(map[string]bool),
	}
}

// Start initializes the pool by spawning warm executors (best-effort).
func (p *Pool) Start(ctx context.Context) error {
	p.ctx, p.cancel = context.WithCancel(ctx)

	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < p.warmTarget; i++ {
		exec, err := p.spawnExecutor(p.ctx)
		if err != nil {
			p.logger.Warn("failed to spawn warm executor", "error", err)
			continue
		}
		p.warm = append(p.warm, exec)
	}

	return nil
}

func (p *Pool) spawnExecutor(ctx context.Context) (*adapter.Executor, error) {
	exec, err := p.adapter.SpawnExecutor(ctx)
	if err != nil {
		return nil, err
	}

	if err := exec.WaitReady(ctx); err != nil {
		_ = exec.Close()
		return nil, fmt.Errorf("executor not ready: %w", err)
	}

	return exec, nil
}

// Execute runs a target. Uses a warm executor if available, otherwise spawns
// one on demand. Returns an error if spawning fails (caller should report to server).
func (p *Pool) Execute(ctx context.Context, executionID, module, target string, arguments []adapter.Argument) error {
	p.mu.Lock()
	if p.shutdown {
		p.mu.Unlock()
		return fmt.Errorf("pool is shut down")
	}

	var exec *adapter.Executor
	if len(p.warm) > 0 {
		exec = p.warm[0]
		p.warm = p.warm[1:]
	}
	p.mu.Unlock()

	// No warm executor available — spawn on demand
	if exec == nil {
		var err error
		exec, err = p.spawnExecutor(ctx)
		if err != nil {
			return fmt.Errorf("failed to spawn executor: %w", err)
		}
	}

	p.mu.Lock()
	p.busy[executionID] = exec
	p.mu.Unlock()

	p.wg.Add(1)
	go p.runExecution(ctx, exec, executionID, module, target, arguments)

	return nil
}

func (p *Pool) runExecution(ctx context.Context, exec *adapter.Executor, executionID, module, target string, arguments []adapter.Argument) {
	defer p.wg.Done()

	// Create a temporary directory for this execution
	workingDir, err := os.MkdirTemp("", "coflux-exec-*")
	if err != nil {
		p.logger.Error("failed to create temp dir for execution", "error", err, "execution_id", executionID)
		p.handler.ReportError(ctx, executionID, "internal", fmt.Sprintf("failed to create temp dir: %s", err.Error()), "")
		p.finishExecution(executionID, exec)
		p.handler.NotifyTerminated(ctx, executionID)
		return
	}

	logger := p.logger.With("execution_id", executionID, "module", module, "target", target)

	// Send execute command
	if err := exec.SendExecute(executionID, module, target, arguments, workingDir); err != nil {
		logger.Error("failed to send execute command", "error", err)
		p.handler.ReportError(ctx, executionID, "internal", err.Error(), "")
		os.RemoveAll(workingDir)
		p.finishExecution(executionID, exec)
		p.handler.NotifyTerminated(ctx, executionID)
		return
	}

	// Handle messages until execution completes
loop:
	for {
		msg, err := exec.Receive(ctx)
		if err != nil {
			// If the execution was aborted by the server (e.g., after suspend),
			// the process was killed intentionally. Don't report an error —
			// the server already knows the execution state.
			p.mu.Lock()
			aborted := p.aborted[executionID]
			p.mu.Unlock()
			if aborted {
				logger.Info("execution aborted")
			logger.Debug("aborted executor exit", "error", err)
			} else {
				logger.Error("failed to receive message", "error", err)
				p.handler.ReportError(ctx, executionID, "internal", err.Error(), "")
			}
			break
		}

		method, id, params, err := adapter.ParseMessage(msg)
		if err != nil {
			logger.Error("failed to parse message", "error", err)
			continue
		}

		switch method {
		case "execution_result":
			p.handleExecutionResult(ctx, executionID, params, logger)
			break loop

		case "execution_error":
			p.handleExecutionError(ctx, executionID, params, logger)
			break loop

		case "log":
			p.handleLog(ctx, executionID, params, logger)

		case "submit_execution", "resolve_reference", "persist_asset", "get_asset", "suspend", "cancel_execution", "download_blob", "upload_blob":
			p.handleRequest(ctx, exec, method, *id, params, logger)

		case "register_group":
			p.handleRegisterGroup(ctx, executionID, params, logger)

		default:
			logger.Warn("unknown message method", "method", method)
		}
	}

	// Wait for the executor process to exit. With one-shot executors, the
	// Python process exits on its own after sending the result. Give it a
	// reasonable timeout before force-killing.
	if err := exec.Wait(5 * time.Second); err != nil {
		logger.Warn("executor exit", "error", err)
	}

	// Clean up temp dir
	os.RemoveAll(workingDir)

	// Remove from busy tracking and notify server that the process has
	// exited. This frees the concurrency slot on the server side.
	p.finishExecution(executionID, nil)
	p.handler.NotifyTerminated(ctx, executionID)

	// Try to maintain warm pool
	p.tryWarm()
}

// finishExecution removes an execution from busy tracking and closes the
// executor if provided.
func (p *Pool) finishExecution(executionID string, execToClose *adapter.Executor) {
	p.mu.Lock()
	delete(p.busy, executionID)
	delete(p.aborted, executionID)
	p.mu.Unlock()

	if execToClose != nil {
		_ = execToClose.Close()
	}
}

// tryWarm attempts to spawn a warm executor if below the warm target and
// concurrency limit. Failures are silently ignored — warming is best-effort.
func (p *Pool) tryWarm() {
	p.mu.Lock()
	if p.shutdown || len(p.warm)+len(p.busy) >= p.concurrency || len(p.warm) >= p.warmTarget {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	exec, err := p.spawnExecutor(p.ctx)
	if err != nil {
		p.logger.Debug("failed to spawn warm executor", "error", err)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Re-check limits after spawning (another execution may have started)
	if p.shutdown || len(p.warm)+len(p.busy) >= p.concurrency || len(p.warm) >= p.warmTarget {
		_ = exec.Close()
		return
	}
	p.warm = append(p.warm, exec)
}

func (p *Pool) handleExecutionResult(ctx context.Context, executionID string, params json.RawMessage, logger *slog.Logger) {
	var result struct {
		ExecutionID string         `json:"execution_id"`
		Result      *adapter.Value `json:"result"`
	}
	if err := json.Unmarshal(params, &result); err != nil {
		logger.Error("failed to parse execution result", "error", err)
		return
	}

	if err := p.handler.ReportResult(ctx, executionID, result.Result); err != nil {
		logger.Error("failed to report result", "error", err)
	}
}

func (p *Pool) handleExecutionError(ctx context.Context, executionID string, params json.RawMessage, logger *slog.Logger) {
	var result struct {
		ExecutionID string `json:"execution_id"`
		Error       struct {
			Type      string `json:"type"`
			Message   string `json:"message"`
			Traceback string `json:"traceback"`
		} `json:"error"`
	}
	if err := json.Unmarshal(params, &result); err != nil {
		logger.Error("failed to parse execution error", "error", err)
		return
	}

	if err := p.handler.ReportError(ctx, executionID, result.Error.Type, result.Error.Message, result.Error.Traceback); err != nil {
		logger.Error("failed to report error", "error", err)
	}
}

func (p *Pool) handleLog(ctx context.Context, executionID string, params json.RawMessage, logger *slog.Logger) {
	var logMsg struct {
		ExecutionID string                    `json:"execution_id"`
		Level       int                       `json:"level"`
		Template    *string                   `json:"template"`
		Values      map[string]*adapter.Value `json:"values"`
	}
	if err := json.Unmarshal(params, &logMsg); err != nil {
		logger.Error("failed to parse log message", "error", err)
		return
	}

	if err := p.handler.RecordLog(ctx, logMsg.ExecutionID, logMsg.Level, logMsg.Template, logMsg.Values); err != nil {
		logger.Error("failed to record log", "error", err)
	}
}

func (p *Pool) handleRegisterGroup(ctx context.Context, executionID string, params json.RawMessage, logger *slog.Logger) {
	var req adapter.RegisterGroupParams
	if err := json.Unmarshal(params, &req); err != nil {
		logger.Error("failed to parse register_group message", "error", err)
		return
	}

	if err := p.handler.RegisterGroup(ctx, req.ExecutionID, req.GroupID, req.Name); err != nil {
		logger.Error("failed to register group", "error", err)
	}
}

func (p *Pool) handleRequest(ctx context.Context, exec *adapter.Executor, method string, id int, params json.RawMessage, logger *slog.Logger) {
	var result any
	var errInfo *adapter.ErrorInfo

	switch method {
	case "submit_execution":
		var req adapter.SubmitExecutionParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		submitResult, err := p.handler.SubmitExecution(ctx, &req)
		if err != nil {
			errInfo = &adapter.ErrorInfo{Code: "submit_error", Message: err.Error()}
		} else {
			result = submitResult
		}

	case "resolve_reference":
		var req adapter.ResolveReferenceParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		resolved, err := p.handler.ResolveReference(ctx, req.ExecutionID, req.TargetExecutionID, req.TimeoutMs)
		if err != nil {
			errInfo = &adapter.ErrorInfo{Code: "resolve_error", Message: err.Error()}
		} else {
			switch resolved.Status {
			case "value":
				result = resolved.Value
			case "error":
				result = map[string]any{
					"status":        "error",
					"error_type":    resolved.ErrorType,
					"error_message": resolved.ErrorMessage,
				}
			case "cancelled":
				result = map[string]any{"status": "cancelled"}
			case "suspended":
				result = map[string]any{"status": "suspended"}
			}
		}

	case "persist_asset":
		var req adapter.PersistAssetParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		assetResult, err := p.handler.PersistAsset(ctx, req.ExecutionID, req.Paths, req.Metadata, req.Entries)
		if err != nil {
			errInfo = &adapter.ErrorInfo{Code: "persist_error", Message: err.Error()}
		} else {
			result = assetResult
		}

	case "get_asset":
		var req adapter.GetAssetParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		entries, err := p.handler.GetAsset(ctx, req.ExecutionID, req.AssetID)
		if err != nil {
			errInfo = &adapter.ErrorInfo{Code: "get_asset_error", Message: err.Error()}
		} else {
			result = map[string]any{"entries": entries}
		}

	case "download_blob":
		var req adapter.DownloadBlobParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		if err := p.handler.DownloadBlob(ctx, req.ExecutionID, req.BlobKey, req.TargetPath); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "download_error", Message: err.Error()}
		} else {
			result = map[string]any{}
		}

	case "upload_blob":
		var req adapter.UploadBlobParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		blobKey, err := p.handler.UploadBlob(ctx, req.ExecutionID, req.SourcePath)
		if err != nil {
			errInfo = &adapter.ErrorInfo{Code: "upload_error", Message: err.Error()}
		} else {
			result = map[string]any{"blob_key": blobKey}
		}

	case "suspend":
		var req adapter.SuspendParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		if err := p.handler.Suspend(ctx, req.ExecutionID, req.ExecuteAfter); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "suspend_error", Message: err.Error()}
		} else {
			result = map[string]any{}
		}

	case "cancel_execution":
		var req adapter.CancelExecutionParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		if err := p.handler.CancelExecution(ctx, req.ExecutionID, req.TargetExecutionID); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "cancel_error", Message: err.Error()}
		} else {
			result = map[string]any{}
		}
	}

	if err := exec.SendResponse(id, result, errInfo); err != nil {
		logger.Error("failed to send response", "error", err, "method", method, "id", id)
	}
}

// Abort terminates an execution by killing its executor process.
func (p *Pool) Abort(executionID string) error {
	p.mu.Lock()
	exec, ok := p.busy[executionID]
	if ok {
		p.aborted[executionID] = true
	}
	p.mu.Unlock()

	if !ok {
		return nil
	}

	_ = exec.Close()
	return nil
}

// Stop shuts down the pool. Closes all warm executors and waits for busy
// executions to finish.
func (p *Pool) Stop() error {
	p.mu.Lock()
	p.shutdown = true
	warm := p.warm
	p.warm = nil
	p.mu.Unlock()

	p.cancel()

	var lastErr error
	for _, exec := range warm {
		if err := exec.Close(); err != nil {
			lastErr = err
		}
	}

	p.wg.Wait()

	return lastErr
}

// Size returns the concurrency limit.
func (p *Pool) Size() int {
	return p.concurrency
}
