package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/bitroot/coflux/cli/internal/adapter"
)

// ExecutionHandler is called when an executor needs to interact with the server
type ExecutionHandler interface {
	// SubmitExecution submits a child execution
	SubmitExecution(ctx context.Context, params *adapter.SubmitExecutionParams) ([]any, error)
	// ResolveReference resolves a reference to get its value
	ResolveReference(ctx context.Context, executionID string, reference []any) (*adapter.Value, error)
	// PersistAsset persists files as an asset
	PersistAsset(ctx context.Context, executionID string, paths []string, metadata map[string]any) (map[string]any, error)
	// GetAsset retrieves asset entries
	GetAsset(ctx context.Context, executionID string, reference []any) (map[string]any, error)
	// DownloadBlob downloads a blob to a local file
	DownloadBlob(ctx context.Context, executionID, blobKey, targetPath string) error
	// UploadBlob uploads a local file as a blob
	UploadBlob(ctx context.Context, executionID, sourcePath string) (string, error)
	// Suspend suspends an execution
	Suspend(ctx context.Context, executionID string, executeAfter *int64) error
	// CancelExecution cancels another execution
	CancelExecution(ctx context.Context, executionID string, targetReference []any) error
	// RegisterGroup registers a group for organizing child executions
	RegisterGroup(ctx context.Context, executionID string, groupID int, name *string) error
	// RecordLog records a log message (level: 0=debug, 1=stdout, 2=info, 3=stderr, 4=warning, 5=error)
	// Template is the message template, values contains serialized values (each is ["raw", data, refs] or ["blob", key, size, refs])
	RecordLog(ctx context.Context, executionID string, level int, template *string, values map[string][]any) error
	// ReportResult reports execution completion
	ReportResult(ctx context.Context, executionID string, result *adapter.Value) error
	// ReportError reports execution failure
	ReportError(ctx context.Context, executionID string, errorType, message, traceback string) error
}

// Pool manages a pool of executor processes
type Pool struct {
	adapter adapter.Adapter
	size    int
	handler ExecutionHandler
	logger  *slog.Logger

	mu        sync.Mutex
	executors []*pooledExecutor
	available chan *pooledExecutor
	shutdown  bool
}

type pooledExecutor struct {
	executor *adapter.Executor
	index    int
	busy     bool
}

// NewPool creates a new executor pool
func NewPool(adp adapter.Adapter, size int, handler ExecutionHandler, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{
		adapter:   adp,
		size:      size,
		handler:   handler,
		logger:    logger,
		executors: make([]*pooledExecutor, 0, size),
		available: make(chan *pooledExecutor, size),
	}
}

// Start initializes the pool with the configured number of executors
func (p *Pool) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < p.size; i++ {
		pe, err := p.spawnExecutor(ctx, i)
		if err != nil {
			for _, existing := range p.executors {
				_ = existing.executor.Close()
			}
			return fmt.Errorf("failed to spawn executor %d: %w", i, err)
		}
		p.executors = append(p.executors, pe)
		p.available <- pe
	}

	return nil
}

func (p *Pool) spawnExecutor(ctx context.Context, index int) (*pooledExecutor, error) {
	exec, err := p.adapter.SpawnExecutor(ctx)
	if err != nil {
		return nil, err
	}

	if err := exec.WaitReady(ctx); err != nil {
		_ = exec.Close()
		return nil, fmt.Errorf("executor not ready: %w", err)
	}

	return &pooledExecutor{
		executor: exec,
		index:    index,
		busy:     false,
	}, nil
}

// Execute runs a target on an available executor
func (p *Pool) Execute(ctx context.Context, executionID, target string, arguments []adapter.Argument) error {
	// Get an available executor
	var pe *pooledExecutor
	select {
	case <-ctx.Done():
		return ctx.Err()
	case pe = <-p.available:
	}

	p.mu.Lock()
	pe.busy = true
	p.mu.Unlock()

	// Run execution in goroutine and handle completion
	go p.runExecution(ctx, pe, executionID, target, arguments)

	return nil
}

func (p *Pool) runExecution(ctx context.Context, pe *pooledExecutor, executionID, target string, arguments []adapter.Argument) {
	defer func() {
		p.mu.Lock()
		pe.busy = false
		shutdown := p.shutdown
		p.mu.Unlock()

		if !shutdown {
			p.available <- pe
		}
	}()

	logger := p.logger.With("executor", pe.index, "execution_id", executionID, "target", target)

	// Send execute command
	if err := pe.executor.SendExecute(executionID, target, arguments); err != nil {
		logger.Error("failed to send execute command", "error", err)
		p.handler.ReportError(ctx, executionID, "internal", err.Error(), "")
		return
	}

	// Handle messages until execution completes
	for {
		msg, err := pe.executor.Receive(ctx)
		if err != nil {
			logger.Error("failed to receive message", "error", err)
			p.handler.ReportError(ctx, executionID, "internal", err.Error(), "")
			return
		}

		method, id, params, err := adapter.ParseMessage(msg)
		if err != nil {
			logger.Error("failed to parse message", "error", err)
			continue
		}

		switch method {
		case "execution_result":
			p.handleExecutionResult(ctx, pe, executionID, params, logger)
			return

		case "execution_error":
			p.handleExecutionError(ctx, pe, executionID, params, logger)
			return

		case "log":
			p.handleLog(ctx, executionID, params, logger)

		case "submit_execution", "resolve_reference", "persist_asset", "get_asset", "suspend", "cancel_execution", "download_blob", "upload_blob":
			p.handleRequest(ctx, pe, method, *id, params, logger)

		case "register_group":
			p.handleRegisterGroup(ctx, executionID, params, logger)

		default:
			logger.Warn("unknown message method", "method", method)
		}
	}
}

func (p *Pool) handleExecutionResult(ctx context.Context, pe *pooledExecutor, executionID string, params json.RawMessage, logger *slog.Logger) {
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

func (p *Pool) handleExecutionError(ctx context.Context, pe *pooledExecutor, executionID string, params json.RawMessage, logger *slog.Logger) {
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
		ExecutionID string           `json:"execution_id"`
		Level       int              `json:"level"`
		Template    *string          `json:"template"`
		Values      map[string][]any `json:"values"`
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

func (p *Pool) handleRequest(ctx context.Context, pe *pooledExecutor, method string, id int, params json.RawMessage, logger *slog.Logger) {
	var result any
	var errInfo *adapter.ErrorInfo

	switch method {
	case "submit_execution":
		var req adapter.SubmitExecutionParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		ref, err := p.handler.SubmitExecution(ctx, &req)
		if err != nil {
			errInfo = &adapter.ErrorInfo{Code: "submit_error", Message: err.Error()}
		} else {
			result = map[string]any{"reference": ref}
		}

	case "resolve_reference":
		var req adapter.ResolveReferenceParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		value, err := p.handler.ResolveReference(ctx, req.ExecutionID, req.Reference)
		if err != nil {
			errInfo = &adapter.ErrorInfo{Code: "resolve_error", Message: err.Error()}
		} else {
			result = value
		}

	case "persist_asset":
		var req adapter.PersistAssetParams
		if err := json.Unmarshal(params, &req); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "parse_error", Message: err.Error()}
			break
		}
		assetResult, err := p.handler.PersistAsset(ctx, req.ExecutionID, req.Paths, req.Metadata)
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
		entries, err := p.handler.GetAsset(ctx, req.ExecutionID, req.Reference)
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
		if err := p.handler.CancelExecution(ctx, req.ExecutionID, req.TargetReference); err != nil {
			errInfo = &adapter.ErrorInfo{Code: "cancel_error", Message: err.Error()}
		} else {
			result = map[string]any{}
		}
	}

	if err := pe.executor.SendResponse(id, result, errInfo); err != nil {
		logger.Error("failed to send response", "error", err, "method", method, "id", id)
	}
}

// Abort terminates an execution by killing its executor
func (p *Pool) Abort(executionID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Find the executor running this execution and kill it
	// Note: In a more sophisticated implementation, we'd track which executor
	// is running which execution. For now, we don't have that tracking.
	return nil
}

// Stop shuts down all executors
func (p *Pool) Stop() error {
	p.mu.Lock()
	p.shutdown = true
	executors := p.executors
	p.executors = nil
	p.mu.Unlock()

	close(p.available)

	var lastErr error
	for _, pe := range executors {
		if err := pe.executor.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Size returns the pool size
func (p *Pool) Size() int {
	return p.size
}

// Available returns the number of available executors
func (p *Pool) Available() int {
	return len(p.available)
}
