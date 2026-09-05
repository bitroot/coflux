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
	"github.com/bitroot/coflux/cli/internal/checkpoint"
	"github.com/bitroot/coflux/cli/internal/config"
	logstore "github.com/bitroot/coflux/cli/internal/log"
	"github.com/bitroot/coflux/cli/internal/metric"
	"github.com/bitroot/coflux/cli/internal/pool"
	"github.com/bitroot/coflux/cli/internal/version"
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
	session string
	logger  *slog.Logger

	// AllowPartialDiscovery keeps the worker running when some of its
	// modules fail to import, serving the targets from the ones that did.
	// Set on reloads under --watch/--dev, where a half-saved file would
	// otherwise exit the worker and leave you restarting it by hand. On
	// first start it stays false: a worker that can't load what it was
	// asked for should say so rather than come up quietly incomplete.
	AllowPartialDiscovery bool

	client      *api.Client
	workspaceID string // resolved external workspace ID
	sessionID   string
	pool        *pool.Pool
	blobs       *blob.Manager
	logs        logstore.Store
	metrics     metric.Store
	tracker     *metric.Tracker
	throttle    *metric.Throttle
	checkpoints *checkpoint.Throttle

	connMu sync.RWMutex
	conn   *api.Connection
	connCh chan struct{} // closed when a new connection is established

	mu         sync.RWMutex
	executions map[string]*executionState

	// Active stream subscriptions, tracked so they can be re-established
	// after a reconnect: the server holds subscriptions in memory only,
	// so a server restart silently drops them — without a re-subscribe,
	// consumers blocked mid-iteration would wait forever.
	streamSubsMu sync.Mutex
	streamSubs   map[streamSubKey]*streamSubscription
}

// streamSubKey identifies a consumer-side stream subscription. The
// subscription ID is allocated by the consumer's adapter and is unique
// within the consumer execution.
type streamSubKey struct {
	executionID    string // consumer execution
	subscriptionID int
}

// streamSubscription remembers the subscribe params plus the next
// sequence the consumer expects (advanced as stream_items are forwarded),
// so a re-subscribe resumes exactly where delivery stopped — no gaps, no
// duplicates.
//
// The credit counters are tracked for the same reason. The adapter's
// counters are cumulative and don't reset when the connection drops, so
// a re-subscribe has to tell the server where the accounting stands.
// Letting the server restart from zero would over-grant the window by
// however many items were in flight; deriving the acked counts from
// `delivered` would under-grant it, and could deadlock a consumer that
// has already drained its queue and so has nothing left to ack.
type streamSubscription struct {
	streamID     string
	nextSequence int
	stride       map[string]any
	prefetch     int
	delivered    int
	ackCount     int
	ackSequence  int
}

type executionState struct {
	status      string // "starting", "executing", "aborting"
	startTime   time.Time
	target      string // Full target name (module/target)
	runID       string // External run ID for logs
	workspaceID string // External workspace ID for logs

	// Buffered result (set when execution finishes, cleared after successful send)
	pendingNotify string // "put_result" or "put_error", empty if nothing pending
	pendingValue  any    // server-format value or error tuple

	// Set when the executor process has exited but notify_terminated hasn't been delivered yet
	pendingTerminated bool
}

// New creates a new worker
func New(cfg *config.Config, adp adapter.Adapter, session string, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		cfg:        cfg,
		adapter:    adp,
		session:    session,
		logger:     logger,
		connCh:     make(chan struct{}),
		executions: make(map[string]*executionState),
		streamSubs: make(map[streamSubKey]*streamSubscription),
	}
}

// getConn returns the current connection (thread-safe)
// Returns nil if not connected - caller must handle this case
func (w *Worker) getConn() *api.Connection {
	w.connMu.RLock()
	defer w.connMu.RUnlock()
	return w.conn
}

// setConn sets the current connection (thread-safe).
// When a non-nil connection is set, any goroutines waiting in waitForConn are unblocked.
func (w *Worker) setConn(conn *api.Connection) {
	w.connMu.Lock()
	defer w.connMu.Unlock()
	w.conn = conn
	if conn != nil {
		close(w.connCh)
		w.connCh = make(chan struct{})
	}
}

// ErrNotConnected is returned when an operation requires an active connection
var ErrNotConnected = fmt.Errorf("not connected to server")

// isConnectionError returns true if the error is due to a lost/closed connection.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNotConnected) {
		return true
	}
	return errors.Is(err, api.ErrConnectionLost)
}

// waitForConn blocks until a connection is available or the context is cancelled.
func (w *Worker) waitForConn(ctx context.Context) (*api.Connection, error) {
	for {
		w.connMu.RLock()
		conn := w.conn
		ch := w.connCh
		w.connMu.RUnlock()
		if conn != nil {
			return conn, nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// requireConn returns the current connection or an error if not connected
func (w *Worker) requireConn() (*api.Connection, error) {
	conn := w.getConn()
	if conn == nil {
		return nil, ErrNotConnected
	}
	return conn, nil
}

// Drain waits for in-flight executions to finish, up to timeout. New
// executions are refused once Drain has been called. Returns the number
// of executions still running when the drain ended. The drain aborts
// early if ctx is cancelled.
//
// The WebSocket stays open so in-flight executions can still report
// results. Cancel the context passed to Run afterwards to tear
// everything down.
func (w *Worker) Drain(ctx context.Context, timeout time.Duration) int {
	if w.pool == nil {
		return 0
	}
	// Signal to the server that this session is draining — the server
	// will stop routing new work here. Any execute messages already in
	// flight (the race window between sending this and the server
	// acting on it) still run normally.
	if conn := w.getConn(); conn != nil {
		if err := conn.Notify("session_draining"); err != nil {
			w.logger.Warn("failed to send session_draining", "error", err)
		}
	}
	return w.pool.Drain(ctx, timeout)
}

// Run starts the worker
func (w *Worker) Run(ctx context.Context, modules []string, register bool) error {
	// Create API client
	w.client = api.NewClient(w.cfg.Host, w.cfg.IsSecure(), w.cfg.Token, w.cfg.Project)

	// Resolve workspace name to external ID
	workspaceID, err := w.resolveWorkspaceID(ctx)
	if err != nil {
		return err
	}
	w.workspaceID = workspaceID

	// Discover targets
	w.logger.Debug("discovering targets", "modules", modules)
	manifest, err := w.adapter.Discover(ctx, modules)
	if err != nil {
		var discoveryErr *adapter.DiscoveryError
		if !errors.As(err, &discoveryErr) || !w.AllowPartialDiscovery || manifest == nil {
			return fmt.Errorf("discovery failed: %w", err)
		}
		// Serve what loaded, but make the gap impossible to miss: targets
		// from the failed modules won't be offered, so runs using them sit
		// unassigned until the next reload fixes the import.
		w.logger.Error("some modules failed to import; continuing without them")
		fmt.Fprintln(os.Stderr, discoveryErr.Details)
	}
	w.logger.Debug("discovered targets", "count", len(manifest.Targets))

	if len(manifest.Targets) == 0 {
		return fmt.Errorf("no targets found in modules %v", modules)
	}

	// Register manifests if requested (before connecting)
	if register {
		w.logger.Debug("registering manifests")
		manifests := w.buildManifests(manifest)
		if err := w.client.RegisterManifests(ctx, w.workspaceID, manifests); err != nil {
			return fmt.Errorf("failed to register manifests: %w", err)
		}
		w.logger.Debug("manifests registered")
	}

	// Create or use existing session
	var sessionID string
	if w.session != "" {
		// Use pre-existing session (for pool-launched workers)
		sessionID = w.session
		w.logger.Debug("using existing session", "session_id", sessionID)
	} else {
		// Create new session
		w.logger.Debug("creating session", "workspace", w.cfg.Workspace)
		provides := config.ParseProvides(w.cfg.Worker.Provides)
		accepts := config.ParseProvides(w.cfg.Worker.Accepts)
		var err error
		sessionID, err = w.client.CreateSession(ctx, w.workspaceID, provides, accepts)
		if err != nil {
			return fmt.Errorf("failed to create session: %w", err)
		}
		w.logger.Debug("created session", "session_id", sessionID)
	}
	w.sessionID = sessionID

	// Build targets map (will be sent via WebSocket after connecting)
	targets := w.buildTargetMap(manifest)

	// Setup blob manager
	cacheDir := filepath.Join(os.TempDir(), fmt.Sprintf("coflux-%s", sessionID), "cache", "blobs")
	stores := w.createBlobStores(ctx, w.sessionID)
	w.blobs = blob.NewManager(stores, cacheDir, w.cfg.Blobs.Threshold)
	w.logger.Debug("blob manager configured", "threshold", w.cfg.Blobs.Threshold, "cache_dir", cacheDir)
	if err := w.blobs.EnsureCacheDir(); err != nil {
		return fmt.Errorf("failed to create blob cache: %w", err)
	}

	// Setup log store
	logURL := w.cfg.HTTPURL() + "/logs"
	logToken := w.sessionID
	if w.cfg.Logs.Token != nil {
		logToken = *w.cfg.Logs.Token
	}
	flushInterval := time.Duration(w.cfg.Logs.FlushInterval * float64(time.Second))
	w.logs = logstore.NewHTTPStore(logURL, logToken, w.cfg.Project, w.cfg.Logs.BatchSize, flushInterval, w.logger)
	defer func() { _ = w.logs.Close() }()

	// Setup metric store
	metricURL := w.cfg.HTTPURL() + "/metrics"
	metricToken := w.sessionID
	if w.cfg.Metrics.Token != nil {
		metricToken = *w.cfg.Metrics.Token
	}
	metricBatchSize := w.cfg.Metrics.BatchSize
	if metricBatchSize <= 0 {
		metricBatchSize = 100
	}
	metricFlushInterval := time.Duration(w.cfg.Metrics.FlushInterval * float64(time.Second))
	if metricFlushInterval <= 0 {
		metricFlushInterval = 500 * time.Millisecond
	}
	metricStore := metric.NewHTTPStore(metricURL, metricToken, w.cfg.Project, metricBatchSize, metricFlushInterval, w.logger)

	w.throttle = metric.NewThrottle(metricStore)
	w.metrics = w.throttle
	w.tracker = metric.NewTracker(w.logger)
	defer func() { _ = w.metrics.Close() }()

	// Checkpoint writes go over the worker connection rather than the metrics
	// HTTP path: they're semantic state, and the pool needs acknowledged
	// flushes at suspend / result / termination rather than a best-effort
	// background drain.
	w.checkpoints = checkpoint.NewThrottle(checkpointSink{w}, checkpoint.DefaultInterval)

	// Determine pool size (default to CPU count + 4)
	poolSize := w.cfg.Worker.Concurrency
	if poolSize <= 0 {
		poolSize = runtime.NumCPU() + 4
	}
	w.logger.Debug("starting executor pool", "size", poolSize)

	// Create executor pool
	w.pool = pool.NewPool(w.adapter, poolSize, w, w.logger)
	if err := w.pool.Start(ctx); err != nil {
		return fmt.Errorf("failed to start executor pool: %w", err)
	}
	defer w.pool.Stop()

	// Run with reconnection loop
	return w.runWithReconnect(ctx, targets)
}

// resolveWorkspaceID resolves a workspace name to its external ID, creating the workspace if needed
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
	id, err := w.client.CreateWorkspace(ctx, w.cfg.Workspace, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create workspace: %w", err)
	}
	return id, nil
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
		w.cfg.Host,
		w.cfg.IsSecure(),
		w.cfg.Project,
		w.workspaceID,
		w.sessionID,
		w.logger,
	)
	conn.RegisterHandler("execute", w.handleExecute)
	conn.RegisterHandler("abort", w.handleAbort)
	conn.RegisterHandler("stream_items", w.handleStreamItems)
	conn.RegisterHandler("stream_closed", w.handleStreamClosed)
	conn.RegisterHandler("stream_demand", w.handleStreamDemand)
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

	// Declare targets and concurrency via WebSocket (now that write loop is running)
	if err := conn.Notify("declare_targets", targets, w.cfg.Worker.Concurrency); err != nil {
		return true, err
	}

	w.mu.RLock()
	hasExecutions := len(w.executions) > 0
	w.mu.RUnlock()
	if hasExecutions {
		w.logger.Info("reconnected", "host", w.cfg.Host)
	} else {
		w.logger.Info("connected", "host", w.cfg.Host)
	}

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

	// Version mismatch is always fatal
	var versionErr *version.VersionMismatchError
	if errors.As(err, &versionErr) {
		return true
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
	for _, fatal := range []string{"session_invalid", "project_not_found", "workspace_mismatch", "version_mismatch"} {
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
		if targets[t.Module] == nil {
			targets[t.Module] = make(map[string][]string)
		}
		targets[t.Module][t.Type] = append(targets[t.Module][t.Type], t.Name)
	}
	return targets
}

func (w *Worker) createBlobStore(ctx context.Context, cfg config.BlobStoreConfig, sessionToken string) (blob.Store, error) {
	switch cfg.Type {
	case "http":
		url := cfg.URL
		if url == "" {
			url = w.cfg.HTTPURL() + "/blobs"
		}
		token := sessionToken
		if cfg.Token != nil {
			token = *cfg.Token
		}
		return blob.NewHTTPStore(url, token, w.cfg.Project), nil
	case "s3":
		return blob.NewS3Store(ctx, cfg.Bucket, cfg.Prefix, cfg.Region)
	default:
		return nil, fmt.Errorf("unknown blob store type: %s", cfg.Type)
	}
}

func (w *Worker) createBlobStores(ctx context.Context, sessionToken string) []blob.Store {
	var stores []blob.Store
	for _, cfg := range w.cfg.Blobs.Stores {
		store, err := w.createBlobStore(ctx, cfg, sessionToken)
		if err != nil {
			w.logger.Error("failed to create blob store", "type", cfg.Type, "error", err)
			continue
		}
		stores = append(stores, store)
	}
	// Default to HTTP store at server
	if len(stores) == 0 {
		stores = append(stores, blob.NewHTTPStore(w.cfg.HTTPURL()+"/blobs", sessionToken, w.cfg.Project))
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

	// Optional timeout_ms (7th param, 0 = no timeout)
	var timeoutMs int64
	if len(params) > 6 && params[6] != nil {
		if v, ok := params[6].(float64); ok {
			timeoutMs = int64(v)
		}
	}

	// Optional streams config (8th param). A map with string keys
	// "buffer" and/or "timeout_ms"; either may be absent. Passed to
	// the adapter so ``cf.stream(...)`` / generator-bodied tasks
	// inherit the caller's override (or the workflow manifest default).
	var streams *adapter.StreamsConfig
	if len(params) > 7 && params[7] != nil {
		if m, ok := params[7].(map[string]any); ok {
			streams = &adapter.StreamsConfig{}
			if v, ok := m["buffer"].(float64); ok {
				buf := int(v)
				streams.Buffer = &buf
			}
			if v, ok := m["timeout_ms"].(float64); ok {
				t := int(v)
				streams.TimeoutMs = &t
			}
		}
	}

	// Optional checkpoints (9th param). The step's effective checkpoint
	// state, resolved server-side and delivered eagerly so the adapter can
	// answer reads without a round-trip. Values are in the same wire form as
	// arguments.
	var checkpoints map[string]*adapter.Value
	if len(params) > 8 && params[8] != nil {
		if m, ok := params[8].(map[string]any); ok && len(m) > 0 {
			checkpoints = make(map[string]*adapter.Value, len(m))
			for name, raw := range m {
				value, err := w.convertValueFromServer(raw)
				if err != nil {
					return fmt.Errorf("checkpoint %q: %w", name, err)
				}
				checkpoints[name] = value
			}
		}
	}

	w.logger.Debug("executing", "execution_id", executionID, "module", moduleName, "target", targetName, "run_id", runID, "timeout_ms", timeoutMs)

	// Track execution
	startTime := time.Now()
	targetKey := moduleName + "/" + targetName
	w.mu.Lock()
	w.executions[executionID] = &executionState{
		status:      "starting",
		startTime:   startTime,
		target:      targetKey,
		runID:       runID,
		workspaceID: workspaceID,
	}
	w.mu.Unlock()

	// Register with metric tracker
	w.tracker.RegisterExecution(executionID, startTime)

	// Send "started" message
	if conn := w.getConn(); conn != nil {
		_ = conn.Notify("started", executionID, map[string]any{})
	}

	// Convert arguments to adapter format
	args, err := w.convertArguments(arguments)
	if err != nil {
		w.logger.Error("failed to convert arguments", "error", err)
		w.ReportError(context.Background(), executionID, "internal", err.Error(), "", nil)
		return nil
	}

	// Update status
	w.mu.Lock()
	if state, ok := w.executions[executionID]; ok {
		state.status = "executing"
	}
	w.mu.Unlock()

	// Execute on pool
	if err := w.pool.Execute(context.Background(), executionID, moduleName, targetName, args, timeoutMs, streams, checkpoints); err != nil {
		w.logger.Error("failed to execute", "error", err, "run_id", runID)
		w.ReportError(context.Background(), executionID, "internal", err.Error(), "", nil)
	}

	return nil
}

func (w *Worker) convertArguments(args []any) ([]adapter.Argument, error) {
	result := make([]adapter.Argument, len(args))
	for i, arg := range args {
		value, err := w.convertValueFromServer(arg)
		if err != nil {
			return nil, fmt.Errorf("argument %d: %w", i, err)
		}
		result[i] = *value
	}
	return result, nil
}

// convertValueFromServer turns a wire-form value array (["raw", data, refs]
// or ["blob", key, size, refs]) into an adapter-side Value struct suitable
// for forwarding to the Python adapter.
func (w *Worker) convertValueFromServer(arg any) (*adapter.Value, error) {
	arr, ok := arg.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array")
	}

	value, err := api.ParseValue(arr)
	if err != nil {
		return nil, err
	}

	adapterRefs, err := w.refsToAdapter(value.References)
	if err != nil {
		return nil, err
	}

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
			return nil, fmt.Errorf("failed to download blob: %w", err)
		}
		return &adapter.Value{
			Type:       "file",
			Format:     "json",
			Path:       path,
			References: adapterRefs,
		}, nil
	}
	return nil, fmt.Errorf("unknown value type: %v", value.Type)
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
		case api.RefTypeInput:
			result[i] = []any{"input", ref.InputID}
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
	w.logger.Debug("handling abort", "execution_id", executionID)

	w.mu.Lock()
	if state, ok := w.executions[executionID]; ok {
		state.status = "aborting"
	}
	w.mu.Unlock()

	// Abort on pool
	return w.pool.Abort(executionID)
}

// handleStreamItems forwards a server-pushed batch of stream items to the
// adapter process owning the target execution. Params: [execution_id,
// subscription_id, items]. Each item arrives as [sequence, value_array]
// and is converted to [sequence, adapter.Value dict] so the Python side
// can deserialize_value it directly.
func (w *Worker) handleStreamItems(params []any) error {
	if len(params) < 3 {
		return fmt.Errorf("stream_items: insufficient params")
	}
	executionID, ok := params[0].(string)
	if !ok {
		return fmt.Errorf("stream_items: execution_id is not a string (got %T)", params[0])
	}
	subscriptionID, ok := params[1].(float64)
	if !ok {
		return fmt.Errorf("stream_items: subscription_id is not a number (got %T)", params[1])
	}
	rawItems, ok := params[2].([]any)
	if !ok {
		return fmt.Errorf("stream_items: items is not an array (got %T)", params[2])
	}

	converted := make([]any, len(rawItems))
	maxSequence := -1
	for i, raw := range rawItems {
		itemArr, ok := raw.([]any)
		if !ok || len(itemArr) != 2 {
			return fmt.Errorf("stream_items: item %d malformed", i)
		}
		value, err := w.convertValueFromServer(itemArr[1])
		if err != nil {
			return fmt.Errorf("stream_items: item %d value: %w", i, err)
		}
		converted[i] = []any{itemArr[0], value}
		if seq, ok := itemArr[0].(float64); ok && int(seq) > maxSequence {
			maxSequence = int(seq)
		}
	}

	// Advance the tracked cursor so a post-reconnect re-subscribe resumes
	// after the last item the consumer was sent, and count the delivery so
	// the re-subscribe can restate the credit accounting.
	if maxSequence >= 0 {
		w.streamSubsMu.Lock()
		if sub, ok := w.streamSubs[streamSubKey{executionID, int(subscriptionID)}]; ok {
			if maxSequence+1 > sub.nextSequence {
				sub.nextSequence = maxSequence + 1
			}
			sub.delivered += len(converted)
		}
		w.streamSubsMu.Unlock()
	}

	return w.pool.PushToExecutor(executionID, "stream_items", map[string]any{
		"execution_id":    executionID,
		"subscription_id": int(subscriptionID),
		"items":           converted,
	})
}

// handleStreamClosed forwards a server-pushed stream-closed notification.
// Params: [execution_id, subscription_id, reason, error_or_null].
//
// `reason` is a string like "complete" / "errored" / "cancelled" /
// "abandoned" / "crashed" / "timeout" / "not_found" — the adapter
// decides how to represent each in its language idiom rather than the
// server fabricating exception types.
func (w *Worker) handleStreamClosed(params []any) error {
	if len(params) < 4 {
		return fmt.Errorf("stream_closed: insufficient params")
	}
	executionID, ok := params[0].(string)
	if !ok {
		return fmt.Errorf("stream_closed: execution_id is not a string (got %T)", params[0])
	}
	subscriptionID, ok := params[1].(float64)
	if !ok {
		return fmt.Errorf("stream_closed: subscription_id is not a number (got %T)", params[1])
	}
	reason, _ := params[2].(string)
	errField := params[3]

	// The subscription is finished — stop tracking it for re-subscribes.
	w.streamSubsMu.Lock()
	delete(w.streamSubs, streamSubKey{executionID, int(subscriptionID)})
	w.streamSubsMu.Unlock()

	forwarded := map[string]any{
		"execution_id":    executionID,
		"subscription_id": int(subscriptionID),
		"reason":          reason,
	}
	if errField != nil {
		forwarded["error"] = errField
	}
	return w.pool.PushToExecutor(executionID, "stream_closed", forwarded)
}

// handleStreamDemand forwards a server-pushed demand grant to the producer
// adapter. Params: [execution_id, index, n]. The producer's StreamDriver
// adds “n“ to its per-stream credit counter and wakes any waiting
// worker thread.
func (w *Worker) handleStreamDemand(params []any) error {
	if len(params) < 3 {
		return fmt.Errorf("stream_demand: insufficient params")
	}
	executionID, ok := params[0].(string)
	if !ok {
		return fmt.Errorf("stream_demand: execution_id is not a string (got %T)", params[0])
	}
	index, ok := params[1].(float64)
	if !ok {
		return fmt.Errorf("stream_demand: index is not a number (got %T)", params[1])
	}
	n, ok := params[2].(float64)
	if !ok {
		return fmt.Errorf("stream_demand: n is not a number (got %T)", params[2])
	}
	return w.pool.PushToExecutor(executionID, "stream_demand", map[string]any{
		"execution_id": executionID,
		"index":        int(index),
		"n":            int(n),
	})
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

func (w *Worker) SubmitExecution(ctx context.Context, params *adapter.SubmitExecutionParams) (map[string]any, error) {
	// Convert arguments back to server format
	module, name := params.Module, params.Target
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
		if params.Retries.BackoffMinMs != nil {
			retriesMap["backoff_min"] = *params.Retries.BackoffMinMs
		}
		if params.Retries.BackoffMaxMs != nil {
			retriesMap["backoff_max"] = *params.Retries.BackoffMaxMs
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

	// Timeout is already in milliseconds from the adapter (0 = no timeout)
	var timeout any
	if params.Timeout > 0 {
		timeout = params.Timeout
	}

	// Streams config (buffer + idle timeout_ms defaults for streams
	// produced by this execution). The buffer key is always present when
	// a config is set — null means explicitly unbounded, which must
	// survive the round-trip (omitting the key would be re-read as the
	// default, strict lockstep).
	var streams any
	if params.Streams != nil {
		s := map[string]any{
			"buffer": params.Streams.Buffer,
		}
		if params.Streams.TimeoutMs != nil {
			s["timeout_ms"] = *params.Streams.TimeoutMs
		}
		streams = s
	}

	// Server expects: module, target, type, arguments, parent_id, group_id, wait_for, cache, defer, memo, delay, retries, recurrent, requires, timeout, streams
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
		timeout,            // timeout
		streams,            // streams
	)
	if err != nil {
		return nil, err
	}

	// Server returns: [execution_id, module, target]
	serverRef, ok := result.([]any)
	if !ok || len(serverRef) < 3 {
		return nil, fmt.Errorf("unexpected submit result: %v", result)
	}
	return map[string]any{
		"execution_id": serverRef[0],
		"module":       serverRef[1],
		"target":       serverRef[2],
	}, nil
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

func (w *Worker) Select(ctx context.Context, params *adapter.SelectParams) (*adapter.SelectResult, error) {
	var result any
	for {
		conn, err := w.waitForConn(ctx)
		if err != nil {
			return nil, err
		}
		result, err = conn.Request(ctx, "select",
			params.Handles,
			params.ExecutionID,
			params.TimeoutMs,
			params.Suspend,
			params.CancelRemaining,
		)
		if err != nil {
			if isConnectionError(err) {
				w.logger.Debug("retrying select after reconnect")
				continue
			}
			return nil, err
		}
		break
	}

	if result == nil {
		return nil, nil
	}

	resultMap, ok := result.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unexpected select result format: %T (%v)", result, result)
	}

	status := getString(resultMap["status"])
	sel := &adapter.SelectResult{Status: status}

	if raw, ok := resultMap["winner"]; ok {
		if f, ok := raw.(float64); ok {
			idx := int(f)
			sel.Winner = &idx
		}
	}

	switch status {
	case "ok":
		valueArr, ok := resultMap["value"].([]any)
		if !ok {
			return nil, fmt.Errorf("ok status missing value tuple: %v", resultMap)
		}
		value, err := api.ParseValue(valueArr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse value: %w", err)
		}
		adapterRefs, err := w.refsToAdapter(value.References)
		if err != nil {
			return nil, fmt.Errorf("failed to convert references: %w", err)
		}
		switch value.Type {
		case api.ValueTypeRaw:
			sel.Value = &adapter.Value{
				Type:       "inline",
				Format:     "json",
				Value:      value.Content,
				References: adapterRefs,
			}
		case api.ValueTypeBlob:
			path, err := w.blobs.Download(value.Key)
			if err != nil {
				return nil, err
			}
			sel.Value = &adapter.Value{
				Type:       "file",
				Format:     "json",
				Path:       path,
				References: adapterRefs,
			}
		default:
			return nil, fmt.Errorf("unknown value type: %s", value.Type)
		}
	case "error":
		if errRaw, ok := resultMap["error"].(map[string]any); ok {
			sel.Error = &adapter.ErrorDetail{
				Type:      getString(errRaw["type"]),
				Message:   getString(errRaw["message"]),
				Traceback: getString(errRaw["traceback"]),
			}
		}
	case "cancelled", "dismissed", "timeout":
		// status-only
	default:
		return nil, fmt.Errorf("unknown select status: %s", status)
	}

	return sel, nil
}

func (w *Worker) SubmitInput(ctx context.Context, params *adapter.SubmitInputParams) (string, error) {
	conn, err := w.requireConn()
	if err != nil {
		return "", err
	}

	// Convert placeholder values to server format
	var serverPlaceholders map[string]any
	if params.Placeholders != nil {
		serverPlaceholders = make(map[string]any)
		for key, val := range params.Placeholders {
			if val != nil {
				sv, err := w.convertValueToServerFormat(val)
				if err != nil {
					return "", fmt.Errorf("failed to convert placeholder %q: %w", key, err)
				}
				serverPlaceholders[key] = sv
			}
		}
	}

	reqParams := []any{
		params.ExecutionID,
		params.Template,
		serverPlaceholders,
		params.Schema,
		params.Key,
		params.Title,
		params.Actions,
		params.Initial,
		params.Requires,
	}

	result, err := conn.Request(ctx, "submit_input", reqParams...)
	if err != nil {
		return "", err
	}

	inputExternalID, ok := result.(string)
	if !ok {
		return "", fmt.Errorf("unexpected submit_input result: %T (%v)", result, result)
	}
	return inputExternalID, nil
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

func (w *Worker) GetAsset(ctx context.Context, executionID string, assetID string) (map[string]any, error) {
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

// SetCheckpoints buffers a checkpoint delta. Delivery is throttled, so this
// returns before the delta necessarily reaches the server — the pool calls
// FlushCheckpoints at the points where that matters.
func (w *Worker) SetCheckpoints(ctx context.Context, executionID string, set map[string]*adapter.Value, reset []string) error {
	return w.checkpoints.Record(ctx, executionID, set, reset)
}

// FlushCheckpoints delivers anything buffered for the execution, returning
// once the server has acknowledged it.
func (w *Worker) FlushCheckpoints(ctx context.Context, executionID string) error {
	return w.checkpoints.Flush(ctx, executionID)
}

// FlushBuffers delivers everything buffered on the execution's behalf —
// checkpoints, metrics and logs — returning once the server has acknowledged
// it. This is what `cf.flush()` reaches, so it has to cover every buffer the
// adapter can fill, not just the one that motivated the call.
//
// All three are attempted regardless of failures: a caller flushing before a
// side effect wants as much delivered as possible, and reporting only the
// first failure would hide the rest.
func (w *Worker) FlushBuffers(ctx context.Context, executionID string) error {
	var errs []error

	if err := w.checkpoints.Flush(ctx, executionID); err != nil {
		errs = append(errs, fmt.Errorf("checkpoints: %w", err))
	}

	// Metrics and logs are batched per worker rather than per execution, so
	// these deliver a superset — harmless, and cheaper than threading
	// per-execution flushing through both stores.
	if err := w.metrics.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("metrics: %w", err))
	}

	if err := w.logs.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("logs: %w", err))
	}

	return errors.Join(errs...)
}

// checkpointSink is the throttle's outbound half. Kept separate from
// Worker.SetCheckpoints — which is the inbound half, feeding the throttle —
// so the two directions don't share a method name.
type checkpointSink struct{ w *Worker }

func (s checkpointSink) SetCheckpoints(ctx context.Context, executionID string, set map[string]*adapter.Value, reset []string) error {
	conn, err := s.w.requireConn()
	if err != nil {
		return err
	}

	// Values are composed here rather than in the adapter, so a value
	// superseded within a throttle window is never uploaded to the blob store.
	composed := make(map[string]any, len(set))
	for name, value := range set {
		serverValue, err := s.w.convertValueToServerFormat(value)
		if err != nil {
			return fmt.Errorf("failed to convert checkpoint %q: %w", name, err)
		}
		composed[name] = serverValue
	}

	if reset == nil {
		reset = []string{}
	}

	// A request rather than a notification: the response is what makes a
	// flush an actual barrier.
	_, err = conn.Request(ctx, "checkpoint_update", executionID, composed, reset)
	return err
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

// StreamRegister declares an execution's k-th stream to the server, which
// allocates the stream's index within its step and decides whether the
// registration resumes a stream paused by a suspend. A request rather
// than a notification: the producer needs the reply (id, index, head)
// before it can embed the handle in its result or append anything.
func (w *Worker) StreamRegister(ctx context.Context, executionID string, position int, buffer *int, timeoutMs *int) (*adapter.StreamRegisterResult, error) {
	conn, err := w.waitForConn(ctx)
	if err != nil {
		return nil, err
	}
	// The wire protocol takes buffer and timeout_ms positionally; nil
	// encodes to JSON null. Server reads [execution_id, position, buffer,
	// timeout_ms].
	var bufferArg, timeoutArg any
	if buffer != nil {
		bufferArg = *buffer
	}
	if timeoutMs != nil {
		timeoutArg = *timeoutMs
	}
	result, err := conn.Request(ctx, "stream_register", executionID, position, bufferArg, timeoutArg)
	if err != nil {
		return nil, err
	}
	m, ok := result.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("stream_register: unexpected result type %T", result)
	}
	id, _ := m["id"].(string)
	index, indexOk := m["index"].(float64)
	if id == "" || !indexOk {
		return nil, fmt.Errorf("stream_register: malformed result %v", result)
	}
	head := -1
	if h, ok := m["head"].(float64); ok {
		head = int(h)
	}
	return &adapter.StreamRegisterResult{ID: id, Index: int(index), Head: head}, nil
}

func (w *Worker) StreamAppend(ctx context.Context, executionID string, index int, sequence int, value *adapter.Value) error {
	conn, err := w.requireConn()
	if err != nil {
		return err
	}
	// Apply blob threshold + upload fragment references just like ReportResult.
	serverValue, err := w.convertValueToServerFormat(value)
	if err != nil {
		return err
	}
	return conn.Notify("stream_append", executionID, index, sequence, serverValue)
}

func (w *Worker) StreamClose(ctx context.Context, executionID string, index int, streamErr *adapter.StreamCloseError, reason *string) error {
	conn, err := w.requireConn()
	if err != nil {
		return err
	}
	var errTuple any
	if streamErr != nil {
		// Match the shape used for put_error: [type, message, frames].
		// Stream closures never carry a retryable flag — retry decisions
		// live at the execution level, not per-stream.
		frames := parseTraceback(streamErr.Traceback)
		errTuple = []any{streamErr.Type, streamErr.Message, frames}
	}
	// Wire: [execution_id, index, error, reason?]. When reason is nil
	// the server infers close kind from error presence (nil→complete,
	// object→errored). A non-nil reason (today only "timeout") is
	// passed through explicitly.
	if reason == nil {
		return conn.Notify("stream_close", executionID, index, errTuple)
	}
	return conn.Notify("stream_close", executionID, index, errTuple, *reason)
}

func (w *Worker) StreamSubscribe(ctx context.Context, executionID string, subscriptionID int, streamID string, fromSequence int, stride map[string]any, prefetch int) error {
	conn, err := w.requireConn()
	if err != nil {
		return err
	}

	// Track before notifying so the subscription is re-established on
	// reconnect (see resubscribeStreams) even if the connection drops
	// immediately after this send.
	w.streamSubsMu.Lock()
	w.streamSubs[streamSubKey{executionID, subscriptionID}] = &streamSubscription{
		streamID:     streamID,
		nextSequence: fromSequence,
		stride:       stride,
		prefetch:     prefetch,
		ackSequence:  fromSequence - 1,
	}
	w.streamSubsMu.Unlock()

	// Params: [subscription_id, consumer_execution_id, stream_id, from_sequence, stride, prefetch]
	return conn.Notify("stream_subscribe", subscriptionID, executionID, streamID, fromSequence, stride, prefetch)
}

// StreamAck forwards a consumer's cumulative progress to the server,
// which frees delivery credit and advances the watermark the producer's
// buffer is measured against.
func (w *Worker) StreamAck(ctx context.Context, executionID string, subscriptionID int, count int, sequence int) error {
	// Record before notifying (as StreamSubscribe does), so an ack issued
	// while disconnected still updates what resubscribeStreams reports.
	// Dropping it here would be unrecoverable: the counters are
	// cumulative, so the adapter never re-sends them, and a consumer that
	// has already drained its queue has nothing left to ack. The server
	// would then resume with stale credit and deliver nothing.
	w.streamSubsMu.Lock()
	if sub, ok := w.streamSubs[streamSubKey{executionID, subscriptionID}]; ok {
		sub.ackCount = count
		sub.ackSequence = sequence
	}
	w.streamSubsMu.Unlock()

	conn, err := w.requireConn()
	if err != nil {
		return err
	}

	// Params: [consumer_execution_id, subscription_id, count, sequence]
	return conn.Notify("stream_ack", executionID, subscriptionID, count, sequence)
}

func (w *Worker) StreamUnsubscribe(ctx context.Context, executionID string, subscriptionID int) error {
	conn, err := w.requireConn()
	if err != nil {
		return err
	}

	w.streamSubsMu.Lock()
	delete(w.streamSubs, streamSubKey{executionID, subscriptionID})
	w.streamSubsMu.Unlock()

	// Server params: [consumer_execution_id, subscription_id]. The consumer
	// id scopes the subscription key server-side, so two adapters in the
	// same session can reuse subscription_id without colliding.
	return conn.Notify("stream_unsubscribe", executionID, subscriptionID)
}

func (w *Worker) Cancel(ctx context.Context, executionID string, handles []adapter.SelectHandle) error {
	conn, err := w.waitForConn(ctx)
	if err != nil {
		return err
	}
	_, err = conn.Request(ctx, "cancel", handles, executionID)
	return err
}

func (w *Worker) RecordLog(ctx context.Context, executionID string, level int, template *string, values map[string]*adapter.Value) error {
	// Get execution context
	w.mu.RLock()
	state, ok := w.executions[executionID]
	w.mu.RUnlock()

	if !ok {
		// Execution not found, just log locally
		w.logger.Warn("log for unknown execution", "execution_id", executionID)
		return nil
	}

	// Process values - convert to server log format (maps with type/data/references)
	var logValues map[string]any
	if values != nil {
		logValues = make(map[string]any, len(values))
		for k, v := range values {
			processed, err := w.convertValueToLogFormat(v)
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

func (w *Worker) DefineMetric(ctx context.Context, executionID string, key string, definition map[string]any) error {
	if throttle, ok := definition["throttle"].(float64); ok {
		w.throttle.SetRate(key, throttle)
	} else {
		w.throttle.DisableRate(key)
	}
	if conn := w.getConn(); conn != nil {
		return conn.Notify("define_metric", executionID, key, definition)
	}
	return nil
}

func (w *Worker) RecordMetric(ctx context.Context, executionID string, key string, value float64, at *float64) error {
	// Get execution context
	w.mu.RLock()
	state, ok := w.executions[executionID]
	w.mu.RUnlock()

	if !ok {
		w.logger.Warn("metric for unknown execution", "execution_id", executionID)
		return nil
	}

	// Process through tracker (validates at, computes auto-at)
	resolvedAt, shouldRecord := w.tracker.Process(executionID, key, at)
	if !shouldRecord {
		return nil
	}

	entry := metric.Entry{
		RunID:       state.runID,
		ExecutionID: executionID,
		WorkspaceID: state.workspaceID,
		Key:         key,
		Value:       value,
		At:          resolvedAt,
	}

	return w.metrics.Record([]metric.Entry{entry})
}

// convertValueToLogFormat converts an adapter value to the log server format.
// This is the log equivalent of convertValueToServerFormat — same input format
// (adapter.Value) but outputs maps instead of tuples for the log HTTP API.
//
// Output formats (maps for server API):
//   - {"type": "raw", "data": ..., "references": [...]}
//   - {"type": "blob", "key": ..., "size": ..., "references": [...]}
func (w *Worker) convertValueToLogFormat(v *adapter.Value) (map[string]any, error) {
	if v == nil {
		return map[string]any{"type": "raw", "references": []any{}}, nil
	}

	// Convert [][]any to []any for processLogReferences
	var rawRefs []any
	for _, ref := range v.References {
		rawRefs = append(rawRefs, any(ref))
	}
	processedRefs, err := w.processLogReferences(rawRefs)
	if err != nil {
		w.logger.Error("failed to process log references", "error", err)
		processedRefs = []any{}
	}

	var data []byte
	switch v.Type {
	case "inline":
		encoded, err := json.Marshal(v.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to encode value: %w", err)
		}
		data = encoded
	case "file":
		data, err = os.ReadFile(v.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to read log value file %s: %w", v.Path, err)
		}
		_ = os.Remove(v.Path)
	default:
		return map[string]any{"type": "raw", "references": []any{}}, nil
	}

	// Apply blob threshold
	if len(data) > w.cfg.Blobs.Threshold {
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

	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
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

		case "input":
			// Input reference: ["input", id]
			if len(refSlice) >= 2 {
				result[i] = map[string]any{
					"type":    "input",
					"inputId": getString(refSlice[1]),
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
		elapsed := time.Since(state.startTime).Round(time.Millisecond)
		w.logger.Info("execution completed", "execution_id", executionID, "target", state.target, "elapsed", elapsed)
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

func (w *Worker) ReportError(ctx context.Context, executionID string, errorType, message, traceback string, retryable *bool) error {
	// Build error tuple matching Python's format: (type, message, frames[, retryable])
	// retryable is nil when no callback configured, true/false when callback ran
	frames := parseTraceback(traceback)
	errorTuple := []any{errorType, message, frames}
	if retryable != nil {
		errorTuple = append(errorTuple, *retryable)
	}

	// Buffer the error on the execution state
	w.mu.Lock()
	state, ok := w.executions[executionID]
	if ok {
		state.pendingNotify = "put_error"
		state.pendingValue = errorTuple
		elapsed := time.Since(state.startTime).Round(time.Millisecond)
		w.logger.Info("execution failed", "execution_id", executionID, "target", state.target, "error_type", errorType, "elapsed", elapsed)
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

func (w *Worker) ReportTimeout(ctx context.Context, executionID string) error {
	// Buffer the timeout on the execution state
	w.mu.Lock()
	state, ok := w.executions[executionID]
	if ok {
		state.pendingNotify = "put_timeout"
		state.pendingValue = nil
		elapsed := time.Since(state.startTime).Round(time.Millisecond)
		w.logger.Info("execution timed out", "execution_id", executionID, "target", state.target, "elapsed", elapsed)
	}
	w.mu.Unlock()

	if !ok {
		return nil
	}

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
		// Null values are always sent as raw, regardless of blob threshold
		if v.Value == nil {
			return []any{"raw", nil, refs}, nil
		}
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
// The result stays buffered (pendingNotify is never cleared) so it can be
// re-sent on reconnect — a successful local WebSocket write doesn't guarantee
// the data reaches the server if the connection drops mid-flight. The server
// deduplicates via has_result?.
// After the write completes, chains to trySendTerminated if the process has
// already exited, relying on sendCh FIFO ordering to ensure the result
// message precedes notify_terminated.
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

	// After the write completes, chain to trySendTerminated if pending.
	// pendingNotify is NOT cleared — the result stays buffered for potential
	// re-send on reconnect.
	onSent := func() {
		w.mu.RLock()
		sendTerminated := false
		if state, ok := w.executions[executionID]; ok && state.pendingTerminated {
			sendTerminated = true
		}
		w.mu.RUnlock()

		if sendTerminated {
			w.trySendTerminated(executionID)
		}
	}

	if err := conn.NotifyWithCallback(onSent, notify, executionID, value); err != nil {
		w.logger.Warn("failed to send result, will retry on reconnect", "execution_id", executionID, "error", err)
		return
	}
}

// trySendTerminated attempts to deliver a pending notify_terminated message.
// Should only be called after the result has been queued to sendCh (either
// via the write callback chain or from flushPending), so that FIFO ordering
// ensures the result message precedes notify_terminated.
//
// The execution entry stays in w.executions (with pendingTerminated = true)
// even after a successful send — "successful" here means the local write
// didn't error, which doesn't guarantee delivery when the underlying TCP
// connection is failing silently. The authoritative signal that the
// server received the termination is the next session message: if the
// execution isn't in the server's known set, handleSession drops it.
// Until then, a reconnect triggers flushPending which re-sends both the
// buffered result (via trySendResult) and this termination.
func (w *Worker) trySendTerminated(executionID string) {
	conn := w.getConn()
	if conn == nil || !conn.IsConnected() {
		return
	}

	w.mu.RLock()
	state, ok := w.executions[executionID]
	if !ok || !state.pendingTerminated {
		w.mu.RUnlock()
		return
	}
	w.mu.RUnlock()

	if err := conn.Notify("notify_terminated", []string{executionID}); err != nil {
		w.logger.Warn("failed to send terminated, will retry on reconnect", "execution_id", executionID, "error", err)
		return
	}
}

// NotifyTerminated is called by the pool after an execution's process has exited.
func (w *Worker) NotifyTerminated(ctx context.Context, executionID string) error {
	// Clean up metric tracking for this execution
	w.tracker.UnregisterExecution(executionID)
	w.throttle.RemoveExecution(executionID)
	// The pool flushes checkpoints before getting here, so anything still
	// buffered couldn't be read anyway — the execution has terminated.
	w.checkpoints.Remove(executionID)

	w.mu.Lock()
	state, ok := w.executions[executionID]
	if !ok {
		// Already pruned (e.g., server no longer cares)
		w.mu.Unlock()
		return nil
	}
	state.pendingTerminated = true
	w.mu.Unlock()

	w.trySendTerminated(executionID)
	return nil
}

// flushPending attempts to deliver all buffered results and terminations.
// Results are sent first; trySendResult chains to trySendTerminated when both
// are pending. Standalone terminations (result already delivered) are sent after.
func (w *Worker) flushPending() {
	w.mu.RLock()
	var pendingResults []string
	var pendingTerminations []string
	for id, state := range w.executions {
		if state.pendingNotify != "" {
			pendingResults = append(pendingResults, id)
		}
		if state.pendingTerminated {
			pendingTerminations = append(pendingTerminations, id)
		}
	}
	w.mu.RUnlock()

	// Send results first. The write callback in trySendResult chains to
	// trySendTerminated if both are pending.
	for _, id := range pendingResults {
		w.trySendResult(id)
	}
	// Send any remaining terminations (where result was already delivered earlier)
	for _, id := range pendingTerminations {
		w.trySendTerminated(id)
	}
}

// handleSession is called when a session message is received (including on reconnect).
// It prunes stale executions, flushes any buffered results, and re-establishes
// stream subscriptions.
func (w *Worker) handleSession(executionIDs []string) {
	// Build set of server-known execution IDs
	known := make(map[string]struct{}, len(executionIDs))
	for _, id := range executionIDs {
		known[id] = struct{}{}
	}

	w.mu.Lock()
	for id := range w.executions {
		if _, ok := known[id]; !ok {
			delete(w.executions, id)
		}
	}
	w.mu.Unlock()

	// Flush any buffered results and terminations
	w.flushPending()

	w.resubscribeStreams(known)
}

// resubscribeStreams re-sends stream_subscribe for every tracked
// subscription whose consumer execution the server still recognises.
// The server holds subscriptions in memory only, so a server restart
// drops them — items keep being appended (they're persisted) but nothing
// would be pushed to the consumer, leaving it blocked forever. Re-sent
// from the next undelivered sequence, so delivery resumes without gaps
// or duplicates. If the server didn't restart, the duplicate subscribe
// is rejected by its already-subscribed guard — a no-op.
func (w *Worker) resubscribeStreams(known map[string]struct{}) {
	conn := w.getConn()
	if conn == nil {
		return
	}

	type resub struct {
		key streamSubKey
		sub streamSubscription
	}

	w.streamSubsMu.Lock()
	resubs := make([]resub, 0, len(w.streamSubs))
	for key, sub := range w.streamSubs {
		if _, ok := known[key.executionID]; ok {
			resubs = append(resubs, resub{key, *sub})
		} else {
			// Consumer execution is gone server-side — drop the orphan.
			delete(w.streamSubs, key)
		}
	}
	w.streamSubsMu.Unlock()

	for _, r := range resubs {
		// Restate the credit accounting: the adapter's counters are
		// cumulative and survive the reconnect, so the server has to pick
		// them up rather than starting a fresh window.
		progress := map[string]any{
			"delivered":   r.sub.delivered,
			"acked_count": r.sub.ackCount,
			"acked_seq":   r.sub.ackSequence,
		}
		err := conn.Notify("stream_subscribe", r.key.subscriptionID, r.key.executionID,
			r.sub.streamID, r.sub.nextSequence, r.sub.stride, r.sub.prefetch, progress)
		if err != nil {
			// Connection dropped again — the next reconnect retries.
			w.logger.Debug("stream re-subscribe failed", "execution_id", r.key.executionID,
				"subscription_id", r.key.subscriptionID, "error", err)
			return
		}
	}
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

		if manifests[t.Module] == nil {
			manifests[t.Module] = make(map[string]any)
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
				"backoff_min": int64(0),
				"backoff_max": int64(0),
			}
			if t.Retries.Limit != nil {
				retriesMap["limit"] = *t.Retries.Limit
			} else {
				retriesMap["limit"] = nil
			}
			if t.Retries.BackoffMinMs != nil {
				retriesMap["backoff_min"] = *t.Retries.BackoffMinMs
			}
			if t.Retries.BackoffMaxMs != nil {
				retriesMap["backoff_max"] = *t.Retries.BackoffMaxMs
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

		// Build timeout (0 = not set, same as delay)
		timeout := int(t.Timeout)

		// Build streams (nil if not set) — keys snake_case to match the
		// Python adapter's wire format for register_manifests. The buffer
		// key is always present when a config is set (null = explicitly
		// unbounded, distinct from the config being absent).
		var streams any
		if t.Streams != nil {
			m := map[string]any{
				"buffer": t.Streams.Buffer,
			}
			if t.Streams.TimeoutMs != nil {
				m["timeout_ms"] = *t.Streams.TimeoutMs
			}
			streams = m
		}

		def := map[string]any{
			"parameters":  buildParameters(t.Parameters),
			"waitFor":     waitFor,
			"cache":       cache,
			"defer":       defer_,
			"delay":       delay,
			"retries":     retries,
			"recurrent":   t.Recurrent,
			"timeout":     timeout,
			"requires":    requires,
			"instruction": instruction,
			"memo":        t.Memo,
			"streams":     streams,
		}

		manifests[t.Module][t.Name] = def
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
