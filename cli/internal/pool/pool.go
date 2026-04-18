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
	// Select waits for the first of one or more handles (executions/inputs) to resolve
	Select(ctx context.Context, params *adapter.SelectParams) (*adapter.SelectResult, error)
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
	// Cancel cancels one or more handles (executions and/or inputs)
	Cancel(ctx context.Context, executionID string, handles []adapter.SelectHandle) error
	// RegisterGroup registers a group for organizing child executions
	RegisterGroup(ctx context.Context, executionID string, groupID int, name *string) error
	// RecordLog records a log message (level: 0=debug, 1=stdout, 2=info, 3=stderr, 4=warning, 5=error)
	// Template is the message template, values contains serialized values (each is ["raw", data, refs] or ["blob", key, size, refs])
	RecordLog(ctx context.Context, executionID string, level int, template *string, values map[string]*adapter.Value) error
	// DefineMetric defines a metric for an execution (metadata only)
	DefineMetric(ctx context.Context, executionID string, key string, definition map[string]any) error
	// RecordMetric records a metric data point
	RecordMetric(ctx context.Context, executionID string, key string, value float64, at *float64) error
	// ReportResult reports execution completion
	ReportResult(ctx context.Context, executionID string, result *adapter.Value) error
	// ReportError reports execution failure
	ReportError(ctx context.Context, executionID string, errorType, message, traceback string, retryable *bool) error
	// ReportTimeout reports that an execution timed out
	ReportTimeout(ctx context.Context, executionID string) error
	// SubmitInput creates an input request and returns its external ID
	SubmitInput(ctx context.Context, params *adapter.SubmitInputParams) (string, error)
	// NotifyTerminated notifies the server that an execution's process has exited
	NotifyTerminated(ctx context.Context, executionID string) error
	// StreamRegister declares a new stream owned by an execution.
	// Sequence is worker-assigned, monotonic per execution.
	StreamRegister(ctx context.Context, executionID string, sequence int) error
	// StreamAppend appends an item to a stream at the given (worker-assigned) position.
	StreamAppend(ctx context.Context, executionID string, sequence int, position int, value *adapter.Value) error
	// StreamClose closes a stream. Error is nil for a clean close, or a (type, message, traceback)
	// triple when the producer's generator raised.
	StreamClose(ctx context.Context, executionID string, sequence int, err *adapter.StreamCloseError) error
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
	draining bool                         // set during Drain: skip warm spawning, but still accept Execute
	shutdown bool                         // set during Stop: refuse Execute entirely
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
// timeoutMs, if > 0, enforces a wall-clock timeout on the execution.
func (p *Pool) Execute(ctx context.Context, executionID, module, target string, arguments []adapter.Argument, timeoutMs int64) error {
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

	// Add to busy and wg under the same mutex so Drain can observe both
	// atomically (otherwise Drain can read busy=0 before wg is incremented,
	// race wg.Wait, and miss an in-flight execution).
	p.mu.Lock()
	p.busy[executionID] = exec
	p.wg.Add(1)
	p.mu.Unlock()

	go p.runExecution(ctx, exec, executionID, module, target, arguments, timeoutMs)

	return nil
}

func (p *Pool) runExecution(ctx context.Context, exec *adapter.Executor, executionID, module, target string, arguments []adapter.Argument, timeoutMs int64) {
	defer p.wg.Done()

	// Create a temporary directory for this execution
	workingDir, err := os.MkdirTemp("", "coflux-exec-*")
	if err != nil {
		p.logger.Error("failed to create temp dir for execution", "error", err, "execution_id", executionID)
		p.handler.ReportError(ctx, executionID, "internal", fmt.Sprintf("failed to create temp dir: %s", err.Error()), "", nil)
		p.finishExecution(executionID, exec)
		p.handler.NotifyTerminated(ctx, executionID)
		return
	}

	logger := p.logger.With("execution_id", executionID, "module", module, "target", target)

	// Send execute command
	if err := exec.SendExecute(executionID, module, target, arguments, workingDir); err != nil {
		logger.Error("failed to send execute command", "error", err)
		p.handler.ReportError(ctx, executionID, "internal", err.Error(), "", nil)
		os.RemoveAll(workingDir)
		p.finishExecution(executionID, exec)
		p.handler.NotifyTerminated(ctx, executionID)
		return
	}

	// Set up timeout: create a derived context that cancels when the timeout fires.
	// exec.Receive() selects on ctx.Done(), so this will interrupt the blocking read.
	execCtx := ctx
	var cancelTimeout context.CancelFunc
	if timeoutMs > 0 {
		logger.Debug("timeout configured", "timeout_ms", timeoutMs)
		execCtx, cancelTimeout = context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
		defer cancelTimeout()
	}
	timedOut := false

	// Handle messages until execution completes
loop:
	for {
		msg, err := exec.Receive(execCtx)
		if err != nil {
			// Check if this was a timeout
			if execCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
				logger.Info("execution timed out", "timeout_ms", timeoutMs)
				timedOut = true
				p.mu.Lock()
				p.aborted[executionID] = true
				p.mu.Unlock()
				_ = exec.Close()
				break
			}

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
				p.handler.ReportError(ctx, executionID, "internal", err.Error(), "", nil)
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
			p.handleExecutionResult(execCtx, executionID, params, logger)
			break loop

		case "execution_error":
			p.handleExecutionError(execCtx, executionID, params, logger)
			break loop

		case "log":
			p.handleLog(execCtx, executionID, params, logger)

		case "define_metric":
			p.handleDefineMetric(execCtx, executionID, params, logger)

		case "metric":
			p.handleMetric(execCtx, executionID, params, logger)

		case "submit_execution", "select", "persist_asset", "get_asset", "suspend", "cancel", "download_blob", "upload_blob", "submit_input":
			p.handleRequest(execCtx, exec, method, *id, params, logger)

		case "register_group":
			p.handleRegisterGroup(execCtx, executionID, params, logger)

		case "stream_register":
			p.handleStreamRegister(execCtx, executionID, params, logger)

		case "stream_append":
			p.handleStreamAppend(execCtx, executionID, params, logger)

		case "stream_close":
			p.handleStreamClose(execCtx, executionID, params, logger)

		default:
			err := fmt.Errorf("unknown message method: %s", method)
			logger.Error("unknown message method", "method", method)
			p.handler.ReportError(ctx, executionID, "internal", err.Error(), "", nil)
			break loop
		}
	}

	// Report timeout to the server if the execution timed out
	if timedOut {
		if err := p.handler.ReportTimeout(ctx, executionID); err != nil {
			logger.Error("failed to report timeout", "error", err)
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
	if p.shutdown || p.draining || len(p.warm)+len(p.busy) >= p.concurrency || len(p.warm) >= p.warmTarget {
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
	if p.shutdown || p.draining || len(p.warm)+len(p.busy) >= p.concurrency || len(p.warm) >= p.warmTarget {
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
			Retryable *bool  `json:"retryable,omitempty"`
		} `json:"error"`
	}
	if err := json.Unmarshal(params, &result); err != nil {
		logger.Error("failed to parse execution error", "error", err)
		return
	}

	if err := p.handler.ReportError(ctx, executionID, result.Error.Type, result.Error.Message, result.Error.Traceback, result.Error.Retryable); err != nil {
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

func (p *Pool) handleDefineMetric(ctx context.Context, executionID string, params json.RawMessage, logger *slog.Logger) {
	var msg struct {
		ExecutionID string         `json:"execution_id"`
		Key         string         `json:"key"`
		Definition  map[string]any `json:"definition"`
	}
	if err := json.Unmarshal(params, &msg); err != nil {
		logger.Error("failed to parse define_metric message", "error", err)
		return
	}

	if err := p.handler.DefineMetric(ctx, msg.ExecutionID, msg.Key, msg.Definition); err != nil {
		logger.Error("failed to define metric", "error", err)
	}
}

func (p *Pool) handleMetric(ctx context.Context, executionID string, params json.RawMessage, logger *slog.Logger) {
	var metricMsg struct {
		ExecutionID string   `json:"execution_id"`
		Key         string   `json:"key"`
		Value       float64  `json:"value"`
		At          *float64 `json:"at,omitempty"`
	}
	if err := json.Unmarshal(params, &metricMsg); err != nil {
		logger.Error("failed to parse metric message", "error", err)
		return
	}

	if err := p.handler.RecordMetric(ctx, metricMsg.ExecutionID, metricMsg.Key, metricMsg.Value, metricMsg.At); err != nil {
		logger.Error("failed to record metric", "error", err)
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

func (p *Pool) handleStreamRegister(ctx context.Context, executionID string, params json.RawMessage, logger *slog.Logger) {
	var req adapter.StreamRegisterParams
	if err := json.Unmarshal(params, &req); err != nil {
		logger.Error("failed to parse stream_register message", "error", err)
		return
	}

	if err := p.handler.StreamRegister(ctx, req.ExecutionID, req.Sequence); err != nil {
		logger.Error("failed to register stream", "error", err)
	}
}

func (p *Pool) handleStreamAppend(ctx context.Context, executionID string, params json.RawMessage, logger *slog.Logger) {
	var req adapter.StreamAppendParams
	if err := json.Unmarshal(params, &req); err != nil {
		logger.Error("failed to parse stream_append message", "error", err)
		return
	}

	if err := p.handler.StreamAppend(ctx, req.ExecutionID, req.Sequence, req.Position, req.Value); err != nil {
		logger.Error("failed to append stream item", "error", err)
	}
}

func (p *Pool) handleStreamClose(ctx context.Context, executionID string, params json.RawMessage, logger *slog.Logger) {
	var req adapter.StreamCloseParams
	if err := json.Unmarshal(params, &req); err != nil {
		logger.Error("failed to parse stream_close message", "error", err)
		return
	}

	if err := p.handler.StreamClose(ctx, req.ExecutionID, req.Sequence, req.Error); err != nil {
		logger.Error("failed to close stream", "error", err)
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

	case "select":
		var req adapter.SelectParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		resolved, err := p.handler.Select(ctx, &req)
		if err != nil {
			errInfo = &adapter.ErrorInfo{Code: "select_error", Message: err.Error()}
		} else if resolved != nil {
			out := map[string]any{"status": resolved.Status}
			if resolved.Winner != nil {
				out["winner"] = *resolved.Winner
			}
			if resolved.Value != nil {
				out["value"] = resolved.Value
			}
			if resolved.Error != nil {
				out["error"] = resolved.Error
			}
			result = out
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

	case "cancel":
		var req adapter.CancelParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		if err := p.handler.Cancel(ctx, req.ExecutionID, req.Handles); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "cancel_error", Message: err.Error()}
		} else {
			result = map[string]any{}
		}

	case "submit_input":
		var req adapter.SubmitInputParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		inputID, err := p.handler.SubmitInput(ctx, &req)
		if err != nil {
			errInfo = &adapter.ErrorInfo{Code: "input_error", Message: err.Error()}
		} else {
			result = map[string]any{"input_id": inputID}
		}

	}

	// If the context was cancelled (e.g., execution timed out), don't
	// bother sending a response — the executor is being killed.
	if ctx.Err() != nil {
		return
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

// Drain marks the pool as draining (stops spawning warm executors),
// closes any existing warm executors, and waits up to timeout for
// in-flight executions to finish on their own. A timeout of 0 means
// wait indefinitely (until ctx is cancelled). Execute calls still
// succeed during drain — late-arriving assignments (from the race
// window between signalling the server and the server acting on it)
// will start fresh executor processes and run normally. Returns the
// number of executions still running when the drain ended (0 if
// everything finished cleanly). Safe to call multiple times.
//
// The drain aborts early if ctx is cancelled.
//
// Stop must still be called afterwards to tear down anything that
// didn't finish within the timeout and to clean up pool resources.
func (p *Pool) Drain(ctx context.Context, timeout time.Duration) int {
	p.mu.Lock()
	warm := p.warm
	p.warm = nil
	busyCount := len(p.busy)
	p.draining = true
	p.mu.Unlock()

	// Warm executors are idle — close them now.
	for _, exec := range warm {
		_ = exec.Close()
	}

	if busyCount == 0 {
		return 0
	}

	drainCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		drainCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return 0
	case <-drainCtx.Done():
		p.mu.Lock()
		remaining := len(p.busy)
		p.mu.Unlock()
		return remaining
	}
}

// Stop shuts down the pool. Closes all warm and busy executors and waits for
// running executions to finish.
func (p *Pool) Stop() error {
	p.mu.Lock()
	p.shutdown = true
	warm := p.warm
	p.warm = nil
	busy := make([]*adapter.Executor, 0, len(p.busy))
	for _, exec := range p.busy {
		busy = append(busy, exec)
	}
	p.mu.Unlock()

	p.cancel()

	var lastErr error
	for _, exec := range warm {
		if err := exec.Close(); err != nil {
			lastErr = err
		}
	}
	for _, exec := range busy {
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
