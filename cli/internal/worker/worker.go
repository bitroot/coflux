package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"mime"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bitroot/coflux/cli/internal/adapter"
	"github.com/bitroot/coflux/cli/internal/api"
	"github.com/bitroot/coflux/cli/internal/blob"
	"github.com/bitroot/coflux/cli/internal/config"
	logstore "github.com/bitroot/coflux/cli/internal/log"
	"github.com/bitroot/coflux/cli/internal/pool"
	"github.com/gorilla/websocket"
)

const (
	heartbeatInterval    = 5 * time.Second
	initialReconnectWait = 1 * time.Second
	maxReconnectWait     = 10 * time.Second
)

// Worker manages the worker lifecycle
type Worker struct {
	cfg     *config.Config
	adapter adapter.Adapter
	logger  *slog.Logger

	client      *api.Client
	workspaceID string // resolved external workspace ID
	sessionID   string
	pool        *pool.Pool
	blobs       *blob.Manager
	logs        logstore.Store

	connMu sync.RWMutex
	conn   *api.Connection

	mu         sync.RWMutex
	executions map[string]*executionState
}

type executionState struct {
	status      string // "starting", "executing", "aborting"
	startTime   time.Time
	runID       string // External run ID for logs
	workspaceID string // External workspace ID for logs

	// Buffered result (set when execution finishes, cleared after successful send)
	pendingNotify string // "put_result" or "put_error", empty if nothing pending
	pendingValue  any    // server-format value or error tuple
}

// New creates a new worker
func New(cfg *config.Config, adp adapter.Adapter, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		cfg:        cfg,
		adapter:    adp,
		logger:     logger,
		executions: make(map[string]*executionState),
	}
}

// getConn returns the current connection (thread-safe)
// Returns nil if not connected - caller must handle this case
func (w *Worker) getConn() *api.Connection {
	w.connMu.RLock()
	defer w.connMu.RUnlock()
	return w.conn
}

// setConn sets the current connection (thread-safe)
func (w *Worker) setConn(conn *api.Connection) {
	w.connMu.Lock()
	defer w.connMu.Unlock()
	w.conn = conn
}

// ErrNotConnected is returned when an operation requires an active connection
var ErrNotConnected = fmt.Errorf("not connected to server")

// requireConn returns the current connection or an error if not connected
func (w *Worker) requireConn() (*api.Connection, error) {
	conn := w.getConn()
	if conn == nil {
		return nil, ErrNotConnected
	}
	return conn, nil
}

// Run starts the worker
func (w *Worker) Run(ctx context.Context, modules []string, register bool) error {
	// Create API client
	w.client = api.NewClient(w.cfg.Server.Host, w.cfg.IsSecure(), w.cfg.Server.Token)

	// Resolve workspace name to external ID
	workspaceID, err := w.resolveWorkspaceID(ctx)
	if err != nil {
		return err
	}
	w.workspaceID = workspaceID

	// Discover targets
	w.logger.Info("discovering targets", "modules", modules)
	manifest, err := w.adapter.Discover(ctx, modules)
	if err != nil {
		return fmt.Errorf("discovery failed: %w", err)
	}
	w.logger.Info("discovered targets", "count", len(manifest.Targets))

	// Register manifests if requested (before connecting)
	if register {
		w.logger.Info("registering manifests")
		manifests := w.buildManifests(manifest)
		if err := w.client.RegisterManifests(ctx, w.workspaceID, manifests); err != nil {
			return fmt.Errorf("failed to register manifests: %w", err)
		}
		w.logger.Info("manifests registered")
	}

	// Create or use existing session
	var sessionID string
	if w.cfg.Session != "" {
		// Use pre-existing session (for pool-launched workers)
		sessionID = w.cfg.Session
		w.logger.Info("using existing session", "session_id", sessionID)
	} else {
		// Create new session
		w.logger.Info("creating session", "workspace", w.cfg.Workspace)
		var err error
		sessionID, err = w.client.CreateSession(ctx, w.workspaceID, w.cfg.Provides, w.cfg.Concurrency)
		if err != nil {
			return fmt.Errorf("failed to create session: %w", err)
		}
		w.logger.Info("created session", "session_id", sessionID)
	}
	w.sessionID = sessionID

	// Build targets map (will be sent via WebSocket after connecting)
	targets := w.buildTargetMap(manifest)

	// Setup blob manager
	cacheDir := filepath.Join(os.TempDir(), fmt.Sprintf("coflux-%s", sessionID), "cache", "blobs")
	stores := w.createBlobStores(ctx)
	w.blobs = blob.NewManager(stores, cacheDir, w.cfg.Blobs.Threshold)
	w.logger.Info("blob manager configured", "threshold", w.cfg.Blobs.Threshold, "cache_dir", cacheDir)
	if err := w.blobs.EnsureCacheDir(); err != nil {
		return fmt.Errorf("failed to create blob cache: %w", err)
	}

	// Setup log store
	logURL := w.cfg.HTTPURL() + "/logs"
	flushInterval := time.Duration(w.cfg.Logs.Store.FlushInterval * float64(time.Second))
	w.logs = logstore.NewHTTPStore(logURL, w.cfg.Logs.Store.BatchSize, flushInterval, w.logger)
	defer func() { _ = w.logs.Close() }()

	// Determine pool size (default to CPU count + 4)
	poolSize := w.cfg.Concurrency
	if poolSize <= 0 {
		poolSize = runtime.NumCPU() + 4
	}
	w.logger.Info("starting executor pool", "size", poolSize)

	// Create executor pool
	w.pool = pool.NewPool(w.adapter, poolSize, w, w.logger)
	if err := w.pool.Start(ctx); err != nil {
		return fmt.Errorf("failed to start executor pool: %w", err)
	}
	defer w.pool.Stop()

	// Run with reconnection loop
	return w.runWithReconnect(ctx, targets)
}

// resolveWorkspaceID resolves a workspace name to its external ID
func (w *Worker) resolveWorkspaceID(ctx context.Context) (string, error) {
	workspaces, err := w.client.GetWorkspaces(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get workspaces: %w", err)
	}
	for id, ws := range workspaces {
		name, _ := ws["name"].(string)
		if name == w.cfg.Workspace {
			return id, nil
		}
	}
	return "", fmt.Errorf("workspace not found: %s", w.cfg.Workspace)
}

// runWithReconnect runs the WebSocket connection with automatic reconnection
func (w *Worker) runWithReconnect(ctx context.Context, targets map[string]map[string][]string) error {
	reconnectWait := initialReconnectWait

	for {
		connected, err := w.runConnection(ctx, targets)

		// Check if context was cancelled
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Check if this is a fatal error that shouldn't trigger reconnection
		if isFatalError(err) {
			return err
		}

		// Reset backoff after a successful connection (transient disconnect)
		if connected {
			reconnectWait = initialReconnectWait
		}

		// Exponential backoff with cap
		reconnectWait = min(reconnectWait*2, maxReconnectWait)

		// Log disconnection and wait before reconnecting
		delay := reconnectWait + time.Duration(rand.Float64()*float64(reconnectWait)/2)
		w.logger.Warn("disconnected from server, reconnecting", "error", err, "delay", delay)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// runConnection establishes and runs a single WebSocket connection.
// Returns (true, err) if a connection was established (even if it later failed),
// or (false, err) if the connection could not be established at all.
func (w *Worker) runConnection(ctx context.Context, targets map[string]map[string][]string) (bool, error) {
	// Create new connection
	conn := api.NewConnection(
		w.cfg.Server.Host,
		w.cfg.IsSecure(),
		w.workspaceID,
		w.sessionID,
		w.logger,
	)
	conn.RegisterHandler("execute", w.handleExecute)
	conn.RegisterHandler("abort", w.handleAbort)
	conn.SetOnSession(w.handleSession)

	if err := conn.Connect(ctx); err != nil {
		return false, err
	}
	defer func() {
		w.setConn(nil)
		_ = conn.Close()
	}()

	// Make connection available to other goroutines
	w.setConn(conn)

	// Run connection loop in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- conn.Run(ctx)
	}()

	// Declare targets via WebSocket (now that write loop is running)
	if err := conn.Notify("declare_targets", targets); err != nil {
		return true, err
	}
	w.logger.Info("connected and declared targets")

	// Start heartbeat for this connection
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()
	go w.heartbeatLoop(heartbeatCtx)

	// Wait for connection loop to complete
	return true, <-errCh
}

// isFatalError checks if an error should prevent reconnection
func isFatalError(err error) bool {
	if err == nil {
		return false
	}

	// Check for WebSocket close errors with specific reasons
	var closeErr *websocket.CloseError
	if ok := errors.As(err, &closeErr); ok {
		reason := closeErr.Text
		switch reason {
		case "session_invalid", "project_not_found", "workspace_mismatch":
			return true
		}
	}

	// Check for error message patterns (in case wrapped differently)
	errMsg := err.Error()
	for _, fatal := range []string{"session_invalid", "project_not_found", "workspace_mismatch"} {
		if strings.Contains(errMsg, fatal) {
			return true
		}
	}

	return false
}

func (w *Worker) buildTargetMap(manifest *adapter.DiscoveryManifest) map[string]map[string][]string {
	// Build targets map: module -> type -> [target_names]
	targets := make(map[string]map[string][]string)
	for _, t := range manifest.Targets {
		// Parse module.target_name format
		module, name := splitTarget(t.Name)
		if targets[module] == nil {
			targets[module] = make(map[string][]string)
		}
		targets[module][t.Type] = append(targets[module][t.Type], name)
	}
	return targets
}

func splitTarget(fullName string) (module, name string) {
	for i := len(fullName) - 1; i >= 0; i-- {
		if fullName[i] == '.' {
			return fullName[:i], fullName[i+1:]
		}
	}
	return "", fullName
}

func (w *Worker) createBlobStores(ctx context.Context) []blob.Store {
	var stores []blob.Store
	for _, cfg := range w.cfg.Blobs.Stores {
		switch cfg.Type {
		case "http":
			stores = append(stores, blob.NewHTTPStore(cfg.URL))
		case "s3":
			s3Store, err := blob.NewS3Store(ctx, cfg.Bucket, cfg.Prefix, cfg.Region)
			if err != nil {
				w.logger.Error("failed to create S3 store", "error", err)
				continue
			}
			stores = append(stores, s3Store)
		}
	}
	// Default to HTTP store at server
	if len(stores) == 0 {
		baseURL := w.cfg.HTTPURL() + "/blobs"
		stores = append(stores, blob.NewHTTPStore(baseURL))
	}
	return stores
}

func (w *Worker) handleExecute(params []any) error {
	if len(params) < 6 {
		return fmt.Errorf("execute: insufficient params")
	}

	executionID := getString(params[0])
	moduleName := getString(params[1])
	targetName := getString(params[2])
	arguments := params[3].([]any)
	runID := getString(params[4])
	workspaceID := getString(params[5])

	w.logger.Info("executing", "execution_id", executionID, "target", targetName, "run_id", runID)

	// Track execution
	w.mu.Lock()
	w.executions[executionID] = &executionState{
		status:      "starting",
		startTime:   time.Now(),
		runID:       runID,
		workspaceID: workspaceID,
	}
	w.mu.Unlock()

	// Convert arguments to adapter format
	args, err := w.convertArguments(arguments)
	if err != nil {
		w.logger.Error("failed to convert arguments", "error", err)
		w.ReportError(context.Background(), executionID, "internal", err.Error(), "")
		return nil
	}

	// Update status
	w.mu.Lock()
	if state, ok := w.executions[executionID]; ok {
		state.status = "executing"
	}
	w.mu.Unlock()

	// Execute on pool
	fullTarget := moduleName + "." + targetName
	if err := w.pool.Execute(context.Background(), executionID, fullTarget, args); err != nil {
		w.logger.Error("failed to execute", "error", err, "run_id", runID)
		w.ReportError(context.Background(), executionID, "internal", err.Error(), "")
	}

	return nil
}

func (w *Worker) convertArguments(args []any) ([]adapter.Argument, error) {
	result := make([]adapter.Argument, len(args))
	for i, arg := range args {
		arr, ok := arg.([]any)
		if !ok {
			return nil, fmt.Errorf("argument %d: expected array", i)
		}

		value, err := api.ParseValue(arr)
		if err != nil {
			return nil, fmt.Errorf("argument %d: %w", i, err)
		}

		// Convert to adapter argument
		adapterRefs, err := w.refsToAdapter(value.References)
		if err != nil {
			return nil, fmt.Errorf("argument %d: %w", i, err)
		}
		switch value.Type {
		case api.ValueTypeRaw:
			result[i] = adapter.Argument{
				Type:       "inline",
				Format:     "json",
				Value:      value.Content,
				References: adapterRefs,
			}
		case api.ValueTypeBlob:
			// Download blob to cache
			path, err := w.blobs.Download(value.Key)
			if err != nil {
				return nil, fmt.Errorf("argument %d: failed to download blob: %w", i, err)
			}
			format := "json"
			result[i] = adapter.Argument{
				Type:       "file",
				Format:     format,
				Path:       path,
				References: adapterRefs,
			}
		}
	}
	return result, nil
}

func (w *Worker) refsToAdapter(refs []api.Reference) ([][]any, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	result := make([][]any, len(refs))
	for i, ref := range refs {
		switch ref.Type {
		case api.RefTypeExecution:
			result[i] = []any{"execution", ref.ExecutionID, ref.Module, ref.Target}
		case api.RefTypeAsset:
			result[i] = []any{"asset", ref.AssetID, ref.Name, ref.TotalCount, ref.TotalSize}
		case api.RefTypeFragment:
			// Download fragment blob to local file so the adapter can deserialize it
			path, err := w.blobs.Download(ref.BlobKey)
			if err != nil {
				return nil, fmt.Errorf("failed to download fragment blob: %w", err)
			}
			result[i] = []any{"fragment", ref.Serializer, path, ref.Size, ref.Metadata}
		}
	}
	return result, nil
}

func (w *Worker) handleAbort(params []any) error {
	if len(params) < 1 {
		return fmt.Errorf("abort: insufficient params")
	}

	executionID := getString(params[0])
	w.logger.Info("handling abort", "execution_id", executionID)

	w.mu.Lock()
	if state, ok := w.executions[executionID]; ok {
		state.status = "aborting"
	}
	w.mu.Unlock()

	// Abort on pool
	return w.pool.Abort(executionID)
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.sendHeartbeat(ctx)
		}
	}
}

func (w *Worker) sendHeartbeat(ctx context.Context) {
	w.mu.RLock()
	statuses := make(map[string]int)
	for id, state := range w.executions {
		// Convert status string to integer (matching Python's ExecutionStatus enum)
		var statusInt int
		switch state.status {
		case "starting":
			statusInt = 0
		case "executing":
			statusInt = 1
		case "aborting":
			statusInt = 2
		case "stopping":
			statusInt = 3
		default:
			statusInt = 0
		}
		statuses[id] = statusInt
	}
	w.mu.RUnlock()

	// Send heartbeat via WebSocket (like Python does with record_heartbeats)
	conn := w.getConn()
	if conn == nil {
		return // Not connected, skip heartbeat
	}
	if err := conn.Notify("record_heartbeats", statuses); err != nil {
		w.logger.Error("heartbeat failed", "error", err)
	}
}

// ExecutionHandler implementation

func (w *Worker) SubmitExecution(ctx context.Context, params *adapter.SubmitExecutionParams) ([]any, error) {
	// Convert arguments back to server format
	module, name := splitTarget(params.Target)
	args, err := w.convertArgumentsToServer(params.Arguments)
	if err != nil {
		return nil, err
	}

	// Convert cache config to server format (snake_case field names)
	var cache any
	if params.Cache != nil {
		cacheMap := map[string]any{
			"params": params.Cache.Params,
		}
		if params.Cache.MaxAgeMs != nil {
			cacheMap["max_age"] = *params.Cache.MaxAgeMs
		} else {
			cacheMap["max_age"] = nil
		}
		if params.Cache.Namespace != nil {
			cacheMap["namespace"] = *params.Cache.Namespace
		} else {
			cacheMap["namespace"] = nil
		}
		if params.Cache.Version != nil {
			cacheMap["version"] = *params.Cache.Version
		} else {
			cacheMap["version"] = nil
		}
		cache = cacheMap
	}

	// Convert defer config to server format
	var deferConfig any
	if params.Defer != nil {
		deferConfig = map[string]any{
			"params": params.Defer.Params,
		}
	}

	// Convert retries config to server format (snake_case field names)
	var retries any
	if params.Retries != nil {
		retriesMap := map[string]any{}
		if params.Retries.Limit != nil {
			retriesMap["limit"] = *params.Retries.Limit
		} else {
			retriesMap["limit"] = nil
		}
		if params.Retries.DelayMinMs != nil {
			retriesMap["delay_min"] = *params.Retries.DelayMinMs
		}
		if params.Retries.DelayMaxMs != nil {
			retriesMap["delay_max"] = *params.Retries.DelayMaxMs
		}
		retries = retriesMap
	}

	// Delay is already in milliseconds from the adapter
	var delay int64
	if params.Delay != nil {
		delay = int64(*params.Delay)
	}

	// Determine target type (default to "task" for backward compatibility)
	targetType := params.Type
	if targetType == "" {
		targetType = "task"
	}

	// Server expects: module, target, type, arguments, parent_id, group_id, wait_for, cache, defer, memo, delay, retries, recurrent, requires
	conn, err := w.requireConn()
	if err != nil {
		return nil, err
	}
	result, err := conn.Request(ctx, "submit",
		module,             // module
		name,               // target
		targetType,         // type
		args,               // arguments
		params.ExecutionID, // parent_id
		params.GroupID,     // group_id
		params.WaitFor,     // wait_for
		cache,              // cache
		deferConfig,        // defer
		params.Memo,        // memo
		delay,              // delay
		retries,            // retries
		params.Recurrent,   // recurrent
		params.Requires,    // requires
	)
	if err != nil {
		return nil, err
	}

	// Parse reference from result
	// Server returns: [execution_id, module, target]
	if serverRef, ok := result.([]any); ok && len(serverRef) >= 3 {
		return []any{"execution", serverRef[0], serverRef[1], serverRef[2]}, nil
	}
	return nil, fmt.Errorf("unexpected submit result: %v", result)
}

func (w *Worker) convertArgumentsToServer(args []adapter.Argument) ([]any, error) {
	result := make([]any, len(args))
	for i := range args {
		v, err := w.convertValueToServerFormat(&args[i])
		if err != nil {
			return nil, err
		}
		result[i] = v
	}
	return result, nil
}

// processReferences uploads fragment file blobs and converts references to server format.
// Fragment references from the adapter contain file paths that need to be uploaded;
// execution and asset references are passed through as-is.
func (w *Worker) processReferences(refs [][]any) ([]any, error) {
	if len(refs) == 0 {
		return []any{}, nil
	}
	result := make([]any, len(refs))
	for i, ref := range refs {
		if len(ref) >= 1 {
			refType, _ := ref[0].(string)
			if refType == "fragment" && len(ref) >= 5 {
				// Fragment: ["fragment", serializer, file_path, size, metadata]
				// Upload the file and replace path with blob key
				filePath, _ := ref[2].(string)
				if filePath != "" {
					key, err := w.blobs.Upload(filePath)
					if err != nil {
						return nil, fmt.Errorf("failed to upload fragment blob: %w", err)
					}
					// Replace file path with blob key
					uploaded := make([]any, len(ref))
					copy(uploaded, ref)
					uploaded[2] = key
					result[i] = uploaded
					continue
				}
			}
		}
		result[i] = ref
	}
	return result, nil
}

func (w *Worker) ResolveReference(ctx context.Context, executionID string, reference []any) (*adapter.Value, error) {
	if len(reference) < 2 {
		return nil, fmt.Errorf("invalid reference")
	}

	refType := getString(reference[0])
	if refType != "execution" {
		return nil, fmt.Errorf("unsupported reference type: %s", refType)
	}

	refID := getString(reference[1])

	// Python params: (target_execution_id, parent_execution_id, timeout_ms)
	conn, err := w.requireConn()
	if err != nil {
		return nil, err
	}
	result, err := conn.Request(ctx, "get_result", refID, executionID, nil)
	if err != nil {
		return nil, err
	}

	// Result is ["value", value_tuple] or ["error", ...] or ["cancelled"] or ["suspended"]
	if arr, ok := result.([]any); ok && len(arr) >= 1 {
		resultType := getString(arr[0])
		switch resultType {
		case "value":
			if len(arr) < 2 {
				return nil, fmt.Errorf("value result missing value tuple: %v", arr)
			}
			valueArr, ok := arr[1].([]any)
			if !ok {
				return nil, fmt.Errorf("value tuple is not an array: %T", arr[1])
			}
			value, err := api.ParseValue(valueArr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse value: %w", err)
			}
			adapterRefs, err := w.refsToAdapter(value.References)
			if err != nil {
				return nil, fmt.Errorf("failed to convert references: %w", err)
			}
			// Convert to adapter value
			switch value.Type {
			case api.ValueTypeRaw:
				return &adapter.Value{
					Type:       "inline",
					Format:     "json",
					Value:      value.Content,
					References: adapterRefs,
				}, nil
			case api.ValueTypeBlob:
				path, err := w.blobs.Download(value.Key)
				if err != nil {
					return nil, err
				}
				return &adapter.Value{
					Type:       "file",
					Format:     "json",
					Path:       path,
					References: adapterRefs,
				}, nil
			default:
				return nil, fmt.Errorf("unknown value type: %s", value.Type)
			}
		case "error":
			return nil, fmt.Errorf("execution failed")
		case "cancelled":
			return nil, fmt.Errorf("execution cancelled")
		case "suspended":
			return nil, fmt.Errorf("execution suspended")
		}
	}

	return nil, fmt.Errorf("unexpected result format: %T", result)
}

func (w *Worker) PersistAsset(ctx context.Context, executionID string, paths []string, metadata map[string]any, preResolved map[string][]any) (map[string]any, error) {
	// Upload each file and create entries
	// Server format: {path: [blob_key, size, metadata]}
	entries := make(map[string][]any)

	// Add pre-resolved entries (existing blob references)
	for path, entry := range preResolved {
		entries[path] = entry
	}

	// Upload local files
	for _, path := range paths {
		key, err := w.blobs.Upload(path)
		if err != nil {
			return nil, fmt.Errorf("failed to upload %s: %w", path, err)
		}
		info, _ := os.Stat(path)
		size := int64(0)
		if info != nil {
			size = info.Size()
		}
		// Create per-file metadata with MIME type detection
		entryMetadata := map[string]any{}
		if ext := filepath.Ext(path); ext != "" {
			if mimeType := mime.TypeByExtension(ext); mimeType != "" {
				entryMetadata["type"] = mimeType
			}
		}
		entries[filepath.Base(path)] = []any{key, size, entryMetadata}
	}

	// Get asset name from metadata if provided
	var name any
	if metadata != nil {
		name = metadata["name"]
	}

	// Python params: (execution_id, name, entries)
	conn, err := w.requireConn()
	if err != nil {
		return nil, err
	}
	result, err := conn.Request(ctx, "put_asset", executionID, name, entries)
	if err != nil {
		return nil, err
	}

	// Result is [external_id, name, total_count, total_size]
	// Adapter expects reference: ["asset", external_id, name, total_count, total_size]
	if serverRef, ok := result.([]any); ok && len(serverRef) >= 1 {
		assetID := getString(serverRef[0])
		ref := make([]any, len(serverRef)+1)
		ref[0] = "asset"
		copy(ref[1:], serverRef)
		return map[string]any{
			"asset_id":  assetID,
			"reference": ref,
		}, nil
	}
	return nil, fmt.Errorf("unexpected result type: %T", result)
}

func (w *Worker) GetAsset(ctx context.Context, executionID string, reference []any) (map[string]any, error) {
	if len(reference) < 2 {
		return nil, fmt.Errorf("invalid asset reference")
	}

	assetID := getString(reference[1])

	// Python params: (asset_id, execution_id)
	conn, err := w.requireConn()
	if err != nil {
		return nil, err
	}
	result, err := conn.Request(ctx, "get_asset", assetID, executionID)
	if err != nil {
		return nil, err
	}

	// Result should be a map of path -> (blob_key, size, metadata)
	entriesMap, ok := result.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unexpected result type: %T", result)
	}

	// Return entries in format expected by adapter: {path: [blob_key, size, metadata]}
	return entriesMap, nil
}

func (w *Worker) Suspend(ctx context.Context, executionID string, executeAfter *int64) error {
	conn, err := w.requireConn()
	if err != nil {
		return err
	}
	// Python params: (execution_id, execute_after_ms)
	return conn.Notify("suspend", executionID, executeAfter)
}

func (w *Worker) DownloadBlob(ctx context.Context, executionID, blobKey, targetPath string) error {
	// Download blob to the target path
	return w.blobs.DownloadTo(blobKey, targetPath)
}

func (w *Worker) UploadBlob(ctx context.Context, executionID, sourcePath string) (string, error) {
	// Upload a local file as a blob
	key, err := w.blobs.Upload(sourcePath)
	if err != nil {
		return "", fmt.Errorf("failed to upload blob: %w", err)
	}
	return key, nil
}

func (w *Worker) RegisterGroup(ctx context.Context, executionID string, groupID int, name *string) error {
	conn, err := w.requireConn()
	if err != nil {
		return err
	}
	// Python params: (parent_id, group_id, name)
	return conn.Notify("register_group", executionID, groupID, name)
}

func (w *Worker) CancelExecution(ctx context.Context, executionID string, targetReference []any) error {
	if len(targetReference) < 2 {
		return fmt.Errorf("invalid target reference")
	}
	conn, err := w.requireConn()
	if err != nil {
		return err
	}
	targetID := getString(targetReference[1])
	// Python params: (target_execution_id,)
	return conn.Notify("cancel", targetID)
}

func (w *Worker) RecordLog(ctx context.Context, executionID string, level int, template *string, values map[string][]any) error {
	// Get execution context
	w.mu.RLock()
	state, ok := w.executions[executionID]
	w.mu.RUnlock()

	if !ok {
		// Execution not found, just log locally
		w.logger.Warn("log for unknown execution", "execution_id", executionID)
		return nil
	}

	// Process values - convert file-based values to raw/blob format
	var logValues map[string]any
	if values != nil {
		logValues = make(map[string]any, len(values))
		for k, v := range values {
			processed, err := w.processLogValue(v)
			if err != nil {
				w.logger.Error("failed to process log value", "key", k, "error", err)
				// Fall back to raw empty value (map format for server)
				logValues[k] = map[string]any{"type": "raw", "references": []any{}}
				continue
			}
			logValues[k] = processed
		}
	}

	// Send to log store
	entry := logstore.Entry{
		RunID:       state.runID,
		ExecutionID: executionID,
		WorkspaceID: state.workspaceID,
		Timestamp:   time.Now().UnixMilli(),
		Level:       level,
		Template:    template,
		Values:      logValues,
	}

	return w.logs.Log(entry)
}

// processLogValue converts executor log values to server format.
// Input formats:
//   - ["raw", data, refs] - inline data from executor
//   - ["file", path, size, refs] - data in temp file from executor
//
// Output formats (maps for server API):
//   - {"type": "raw", "data": ..., "references": [...]}
//   - {"type": "blob", "key": ..., "size": ..., "references": [...]}
func (w *Worker) processLogValue(value []any) (map[string]any, error) {
	if len(value) < 3 {
		return map[string]any{"type": "raw", "references": []any{}}, nil
	}

	valueType := getString(value[0])
	var data []byte
	var refs []any

	switch valueType {
	case "raw":
		// Inline data - may need to convert to blob if over threshold
		refs = getSlice(value[2])
		// Data is already JSON-decoded, re-encode to check size
		encoded, err := json.Marshal(value[1])
		if err != nil {
			return nil, fmt.Errorf("failed to encode raw value: %w", err)
		}
		data = encoded

	case "file":
		// Data is in a temp file - read and clean up
		if len(value) < 4 {
			return map[string]any{"type": "raw", "references": []any{}}, nil
		}
		path := getString(value[1])
		refs = getSlice(value[3])

		var err error
		data, err = os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read log value file %s: %w", path, err)
		}
		// Clean up temp file
		_ = os.Remove(path)

	default:
		// Unknown type, return as raw with null data
		return map[string]any{"type": "raw", "references": []any{}}, nil
	}

	// Process fragment references - read files and upload to blob store
	processedRefs, err := w.processLogReferences(refs)
	if err != nil {
		w.logger.Error("failed to process log references", "error", err)
		processedRefs = []any{} // Fall back to empty refs
	}

	// Apply blob threshold
	if len(data) > w.cfg.Blobs.Threshold {
		// Upload to blob store
		key, err := w.blobs.UploadData(data)
		if err != nil {
			return nil, fmt.Errorf("failed to upload log value blob: %w", err)
		}
		return map[string]any{
			"type":       "blob",
			"key":        key,
			"size":       len(data),
			"references": processedRefs,
		}, nil
	}

	// Return as inline - need to decode the JSON data back to any
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		// If it can't be decoded, return as raw bytes (shouldn't happen)
		return nil, fmt.Errorf("failed to decode log value: %w", err)
	}
	return map[string]any{
		"type":       "raw",
		"data":       decoded,
		"references": processedRefs,
	}, nil
}

// processLogReferences processes fragment references in log values.
// Fragments from the executor have file paths instead of blob keys.
// This reads the files, uploads to blob store if needed, and returns updated refs.
// Server expects references as maps with specific keys.
func (w *Worker) processLogReferences(refs []any) ([]any, error) {
	if len(refs) == 0 {
		return []any{}, nil
	}

	result := make([]any, len(refs))
	for i, ref := range refs {
		refSlice, ok := ref.([]any)
		if !ok || len(refSlice) < 1 {
			// Try to pass through as map if it already is one
			if refMap, ok := ref.(map[string]any); ok {
				result[i] = refMap
			} else {
				result[i] = ref
			}
			continue
		}

		refType := getString(refSlice[0])
		switch refType {
		case "fragment":
			if len(refSlice) >= 5 {
				// Fragment reference: ["fragment", serializer, path, size, metadata]
				serializer := getString(refSlice[1])
				path := getString(refSlice[2])
				size := getInt(refSlice[3])
				metadata := refSlice[4]

				// Ensure metadata is a map (frontend expects it)
				metadataMap, ok := metadata.(map[string]any)
				if !ok {
					metadataMap = map[string]any{}
				}

				// Read the fragment file
				data, err := os.ReadFile(path)
				if err != nil {
					w.logger.Error("failed to read fragment file", "path", path, "error", err)
					result[i] = map[string]any{"type": "fragment", "format": serializer, "blobKey": "", "size": size, "metadata": metadataMap}
					continue
				}
				// Clean up temp file
				os.Remove(path)

				// Upload to blob store
				key, err := w.blobs.UploadData(data)
				if err != nil {
					w.logger.Error("failed to upload fragment blob", "error", err)
					result[i] = map[string]any{"type": "fragment", "format": serializer, "blobKey": "", "size": size, "metadata": metadataMap}
					continue
				}

				// Return reference as map with server-expected keys
				result[i] = map[string]any{
					"type":     "fragment",
					"format":   serializer,
					"blobKey":  key,
					"size":     size,
					"metadata": metadataMap,
				}
			}

		case "execution":
			// Execution reference: ["execution", id, module, target]
			if len(refSlice) >= 4 {
				result[i] = map[string]any{
					"type":        "execution",
					"executionId": getString(refSlice[1]),
					"module":      refSlice[2],
					"target":      refSlice[3],
				}
			}

		case "asset":
			// Asset reference: ["asset", id, name, total_count, total_size]
			if len(refSlice) >= 5 {
				result[i] = map[string]any{
					"type":       "asset",
					"assetId":    getString(refSlice[1]),
					"name":       refSlice[2],
					"totalCount": refSlice[3],
					"totalSize":  refSlice[4],
				}
			}

		default:
			result[i] = ref
		}
	}

	return result, nil
}

func getSlice(v any) []any {
	if s, ok := v.([]any); ok {
		return s
	}
	return nil
}

func getInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

func (w *Worker) ReportResult(ctx context.Context, executionID string, result *adapter.Value) error {
	// Convert result to server format eagerly (blob uploads happen here, outside lock)
	serverValue, err := w.convertValueToServerFormat(result)
	if err != nil {
		return err
	}

	// Buffer the result on the execution state
	w.mu.Lock()
	state, ok := w.executions[executionID]
	if ok {
		state.pendingNotify = "put_result"
		state.pendingValue = serverValue
	}
	w.mu.Unlock()

	if !ok {
		// Execution already pruned (e.g., server no longer cares) - discard
		return nil
	}

	// Try to send immediately
	w.trySendResult(executionID)
	return nil
}

func (w *Worker) ReportError(ctx context.Context, executionID string, errorType, message, traceback string) error {
	// Build error tuple matching Python's format: (type, message, frames)
	frames := parseTraceback(traceback)
	errorTuple := []any{errorType, message, frames}

	// Buffer the error on the execution state
	w.mu.Lock()
	state, ok := w.executions[executionID]
	if ok {
		state.pendingNotify = "put_error"
		state.pendingValue = errorTuple
	}
	w.mu.Unlock()

	if !ok {
		// Execution already pruned - discard
		return nil
	}

	// Try to send immediately
	w.trySendResult(executionID)
	return nil
}

// convertValueToServerFormat converts an adapter value to server wire format.
// Applies blob threshold and uploads fragment references. Should be called outside of any lock.
func (w *Worker) convertValueToServerFormat(v *adapter.Value) (any, error) {
	if v == nil {
		return []any{"raw", nil, []any{}}, nil
	}
	refs, err := w.processReferences(v.References)
	if err != nil {
		return nil, err
	}
	switch v.Type {
	case "inline":
		// Check blob threshold - encode to JSON to measure size
		encoded, err := json.Marshal(v.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to encode value: %w", err)
		}
		if len(encoded) > w.cfg.Blobs.Threshold {
			key, err := w.blobs.UploadData(encoded)
			if err != nil {
				return nil, fmt.Errorf("failed to upload blob: %w", err)
			}
			return []any{"blob", key, len(encoded), refs}, nil
		}
		return []any{"raw", v.Value, refs}, nil
	case "file":
		key, err := w.blobs.Upload(v.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to upload blob: %w", err)
		}
		info, _ := os.Stat(v.Path)
		size := int64(0)
		if info != nil {
			size = info.Size()
		}
		return []any{"blob", key, size, refs}, nil
	default:
		return []any{"raw", nil, []any{}}, nil
	}
}

// trySendResult attempts to deliver the buffered result for an execution.
// If the send fails (e.g., disconnected), the result stays buffered for retry on reconnect.
func (w *Worker) trySendResult(executionID string) {
	conn := w.getConn()
	if conn == nil || !conn.IsConnected() {
		return
	}

	// Read pending data under read lock
	w.mu.RLock()
	state, ok := w.executions[executionID]
	if !ok || state.pendingNotify == "" {
		w.mu.RUnlock()
		return
	}
	notify := state.pendingNotify
	value := state.pendingValue
	w.mu.RUnlock()

	// Attempt to send. Notify is fire-and-forget (queues to send channel),
	// so we can't confirm delivery. We intentionally leave the execution in
	// the map with its pending data intact. On reconnect, handleSession will
	// either prune it (server no longer lists it → result was delivered) or
	// reflush it (server still lists it → result was lost in transit).
	if err := conn.Notify(notify, executionID, value); err != nil {
		w.logger.Warn("failed to send result, will retry on reconnect", "execution_id", executionID, "error", err)
		return
	}
	if err := conn.Notify("notify_terminated", []string{executionID}); err != nil {
		w.logger.Warn("failed to send terminated, will retry on reconnect", "execution_id", executionID, "error", err)
		return
	}
}

// flushPendingResults attempts to deliver all buffered results.
func (w *Worker) flushPendingResults() {
	w.mu.RLock()
	var pending []string
	for id, state := range w.executions {
		if state.pendingNotify != "" {
			pending = append(pending, id)
		}
	}
	w.mu.RUnlock()

	for _, id := range pending {
		w.trySendResult(id)
	}
}

// handleSession is called when a session message is received (including on reconnect).
// It prunes stale executions and flushes any buffered results.
func (w *Worker) handleSession(executionIDs []string) {
	// Build set of server-known execution IDs
	known := make(map[string]struct{}, len(executionIDs))
	for _, id := range executionIDs {
		known[id] = struct{}{}
	}

	// Prune executions not in the server's list (result was delivered, or
	// server no longer cares about them).
	w.mu.Lock()
	for id := range w.executions {
		if _, ok := known[id]; !ok {
			delete(w.executions, id)
		}
	}
	w.mu.Unlock()

	// Flush any buffered results
	w.flushPendingResults()
}

func getString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// buildManifests builds the manifests map for registering with the server
// This only includes workflows (not tasks) as manifests define what's visible in Studio
func (w *Worker) buildManifests(manifest *adapter.DiscoveryManifest) map[string]map[string]any {
	manifests := make(map[string]map[string]any)

	for _, t := range manifest.Targets {
		if t.Type != "workflow" {
			continue
		}

		module, name := splitTarget(t.Name)
		if manifests[module] == nil {
			manifests[module] = make(map[string]any)
		}

		// Build waitFor as list (empty if nil)
		waitFor := []int{}
		if arr, ok := t.WaitFor.([]any); ok {
			for _, v := range arr {
				if n, ok := v.(float64); ok {
					waitFor = append(waitFor, int(n))
				}
			}
		}

		// Build cache (nil if not set) - uses snake_case for server
		var cache any
		if t.Cache != nil {
			cacheMap := map[string]any{
				"params": t.Cache.Params,
			}
			if t.Cache.MaxAgeMs != nil {
				cacheMap["max_age"] = *t.Cache.MaxAgeMs
			} else {
				cacheMap["max_age"] = nil
			}
			if t.Cache.Namespace != nil {
				cacheMap["namespace"] = *t.Cache.Namespace
			} else {
				cacheMap["namespace"] = nil
			}
			if t.Cache.Version != nil {
				cacheMap["version"] = *t.Cache.Version
			} else {
				cacheMap["version"] = nil
			}
			cache = cacheMap
		}

		// Build defer (nil if not set)
		var defer_ any
		if t.Defer != nil {
			defer_ = map[string]any{
				"params": t.Defer.Params,
			}
		}

		// Delay is already in milliseconds from the adapter (0 if not set - server requires integer, not nil)
		delay := 0
		if t.Delay != nil {
			delay = int(*t.Delay)
		}

		// Build retries (nil if not set) - uses snake_case for server
		var retries any
		if t.Retries != nil {
			retriesMap := map[string]any{
				"delay_min": int64(0),
				"delay_max": int64(0),
			}
			if t.Retries.Limit != nil {
				retriesMap["limit"] = *t.Retries.Limit
			} else {
				retriesMap["limit"] = nil
			}
			if t.Retries.DelayMinMs != nil {
				retriesMap["delay_min"] = *t.Retries.DelayMinMs
			}
			if t.Retries.DelayMaxMs != nil {
				retriesMap["delay_max"] = *t.Retries.DelayMaxMs
			}
			retries = retriesMap
		}

		// Build requires (nil if not set, like Python does)
		var requires any
		if len(t.Requires) > 0 {
			requires = t.Requires
		}

		// Build instruction (nil if not set)
		var instruction any
		if t.Instruction != nil {
			instruction = *t.Instruction
		}

		def := map[string]any{
			"parameters":  buildParameters(t.Parameters),
			"waitFor":     waitFor,
			"cache":       cache,
			"defer":       defer_,
			"delay":       delay,
			"retries":     retries,
			"recurrent":   t.Recurrent,
			"requires":    requires,
			"instruction": instruction,
		}

		manifests[module][name] = def
	}

	return manifests
}

func buildParameters(params []adapter.Parameter) []map[string]any {
	result := make([]map[string]any, len(params))
	for i, p := range params {
		param := map[string]any{
			"name": p.Name,
		}
		if p.Annotation != nil {
			param["annotation"] = *p.Annotation
		}
		if p.Default != nil {
			param["default"] = *p.Default
		}
		result[i] = param
	}
	return result
}

// tracebackFrameRegex matches Python traceback frame lines:
// '  File "path/to/file.py", line 123, in function_name'
var tracebackFrameRegex = regexp.MustCompile(`^\s*File "([^"]+)", line (\d+), in (.+)$`)

// parseTraceback parses a Python traceback string into structured frames.
// Returns a list of [filename, lineno, function_name, source_line] tuples.
func parseTraceback(traceback string) []any {
	if traceback == "" {
		return []any{}
	}

	lines := strings.Split(traceback, "\n")
	var frames []any

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		matches := tracebackFrameRegex.FindStringSubmatch(line)
		if matches == nil {
			continue
		}

		filename := matches[1]
		lineno, _ := strconv.Atoi(matches[2])
		funcName := matches[3]

		// Try to get the source line (next line if it's indented)
		var sourceLine string
		if i+1 < len(lines) {
			nextLine := lines[i+1]
			// Source lines are indented with 4+ spaces
			if strings.HasPrefix(nextLine, "    ") {
				sourceLine = strings.TrimSpace(nextLine)
				i++ // Skip the source line in next iteration
			}
		}

		frames = append(frames, []any{filename, lineno, funcName, sourceLine})
	}

	// If we couldn't parse any frames, return the whole traceback as a single frame
	if len(frames) == 0 && traceback != "" {
		return []any{[]any{"", 0, "", traceback}}
	}

	return frames
}
