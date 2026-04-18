package adapter

import (
	"encoding/json"
)

// DiscoveryManifest is the output of the discover command
type DiscoveryManifest struct {
	Targets []TargetDefinition `json:"targets"`
}

// TargetDefinition describes a discovered task or workflow
type TargetDefinition struct {
	Module      string              `json:"module"`
	Name        string              `json:"name"`
	Type        string              `json:"type"` // "task" or "workflow"
	Parameters  []Parameter         `json:"parameters"`
	Cache       *CacheConfig        `json:"cache,omitempty"`
	Retries     *RetriesConfig      `json:"retries,omitempty"`
	Defer       *DeferConfig        `json:"defer,omitempty"`
	Delay       *float64            `json:"delay,omitempty"`
	WaitFor     any                 `json:"wait_for,omitempty"` // true or list of param indices
	Memo        any                 `json:"memo,omitempty"`     // true or list of param indices
	Requires    map[string][]string `json:"requires,omitempty"`
	Recurrent   bool                `json:"recurrent,omitempty"`
	Timeout     int64               `json:"timeout,omitempty"` // timeout in milliseconds
	IsStub      bool                `json:"is_stub,omitempty"`
	Instruction *string             `json:"instruction,omitempty"`
}

// Parameter describes a function parameter
type Parameter struct {
	Name       string  `json:"name"`
	Annotation *string `json:"annotation,omitempty"`
	Default    *string `json:"default,omitempty"` // JSON-encoded default value
}

// CacheConfig describes caching behavior
type CacheConfig struct {
	Params    any     `json:"params,omitempty"` // true or list of param indices
	MaxAgeMs  *int64  `json:"max_age_ms,omitempty"`
	Namespace *string `json:"namespace,omitempty"`
	Version   *string `json:"version,omitempty"`
}

// RetriesConfig describes retry behavior
type RetriesConfig struct {
	Limit        *int   `json:"limit,omitempty"`
	BackoffMinMs *int64 `json:"backoff_min_ms,omitempty"`
	BackoffMaxMs *int64 `json:"backoff_max_ms,omitempty"`
}

// DeferConfig describes defer behavior
type DeferConfig struct {
	Params any `json:"params,omitempty"` // true or list of param indices
}

// ExecuteRequest is sent from CLI to executor to run a target
type ExecuteRequest struct {
	Method string               `json:"method"` // "execute"
	Params ExecuteRequestParams `json:"params"`
}

// ExecuteRequestParams contains execution parameters
type ExecuteRequestParams struct {
	ExecutionID string     `json:"execution_id"`
	Module      string     `json:"module"`
	Target      string     `json:"target"`
	Arguments   []Argument `json:"arguments"`
	WorkingDir  string     `json:"working_dir,omitempty"`
}

// Argument is the same structure as Value (used for arguments to distinguish context)
type Argument = Value

// Response is sent from CLI to executor in response to a request
type Response struct {
	ID     int        `json:"id"`
	Result any        `json:"result"`
	Error  *ErrorInfo `json:"error,omitempty"`
}

// ErrorInfo describes an error in a response
type ErrorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// SelectResult represents the outcome of a select call where a handle
// resolved. A nil *SelectResult means the select wait itself expired
// without any handle resolving.
// Status is one of "ok", "error", "cancelled", "dismissed", or "timeout"
// (the last meaning the resolved handle's execution timed out).
type SelectResult struct {
	Winner *int         // index into handles
	Status string       // "ok" | "error" | "cancelled" | "dismissed" | "timeout"
	Value  *Value       // set when Status == "ok"
	Error  *ErrorDetail // set when Status == "error"
}

// SelectHandle identifies a single handle in a select call.
type SelectHandle struct {
	Type string `json:"type"` // "execution" or "input"
	ID   string `json:"id"`
}

// ReadyMessage is sent by executor when it's ready for work
type ReadyMessage struct {
	Method string       `json:"method"` // "ready"
	Params *ReadyParams `json:"params,omitempty"`
}

// ReadyParams contains the ready message parameters
type ReadyParams struct {
	Version string `json:"version"`
}

// ExecutionResultMessage is sent when execution completes successfully
type ExecutionResultMessage struct {
	Method string                `json:"method"` // "execution_result"
	Params ExecutionResultParams `json:"params"`
}

// ExecutionResultParams contains the execution result
type ExecutionResultParams struct {
	ExecutionID string `json:"execution_id"`
	Result      *Value `json:"result,omitempty"`
}

// Value represents a serialized value
type Value struct {
	Type       string         `json:"type"`   // "inline" or "file"
	Format     string         `json:"format"` // serialization format
	Value      any            `json:"value,omitempty"`
	Path       string         `json:"path,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	References [][]any        `json:"references,omitempty"`
}

// ExecutionErrorMessage is sent when execution fails
type ExecutionErrorMessage struct {
	Method string               `json:"method"` // "execution_error"
	Params ExecutionErrorParams `json:"params"`
}

// ExecutionErrorParams contains error details
type ExecutionErrorParams struct {
	ExecutionID string      `json:"execution_id"`
	Error       ErrorDetail `json:"error"`
}

// ErrorDetail describes an execution error
type ErrorDetail struct {
	Type      string `json:"type"` // "exception", "cancelled", etc.
	Message   string `json:"message"`
	Traceback string `json:"traceback,omitempty"`
	Retryable *bool  `json:"retryable,omitempty"` // nil = default (true), false = don't retry
}

// LogMessage is sent during execution for logging
type LogMessage struct {
	Method string    `json:"method"` // "log"
	Params LogParams `json:"params"`
}

// LogParams contains log details
type LogParams struct {
	ExecutionID string `json:"execution_id"`
	Level       string `json:"level"` // "debug", "info", "warning", "error"
	Message     string `json:"message"`
}

// ExecutorRequest represents a request from executor to CLI (requires response)
type ExecutorRequest struct {
	ID     int    `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params"`
}

// SubmitExecutionParams for submit_execution request
type SubmitExecutionParams struct {
	ExecutionID string              `json:"execution_id"` // parent execution
	Module      string              `json:"module"`
	Target      string              `json:"target"`
	Type        string              `json:"type,omitempty"` // "task" or "workflow" (default: "task")
	Arguments   []Argument          `json:"arguments"`
	WaitFor     any                 `json:"wait_for,omitempty"` // reference to wait for
	GroupID     *int                `json:"group_id,omitempty"` // group to associate with
	Cache       *CacheConfig        `json:"cache,omitempty"`
	Defer       *DeferConfig        `json:"defer,omitempty"`
	Memo        any                 `json:"memo,omitempty"`  // true or list of param indices
	Delay       *float64            `json:"delay,omitempty"` // delay in seconds
	Retries     *RetriesConfig      `json:"retries,omitempty"`
	Recurrent   bool                `json:"recurrent,omitempty"`
	Requires    map[string][]string `json:"requires,omitempty"`
	Timeout     int64               `json:"timeout,omitempty"` // timeout in milliseconds
}

// SubmitExecutionResult is the response to submit_execution
type SubmitExecutionResult struct {
	Reference []any `json:"reference"`
}

// SelectParams for select request
type SelectParams struct {
	ExecutionID     string         `json:"execution_id"`
	Handles         []SelectHandle `json:"handles"`
	TimeoutMs       *int64         `json:"timeout_ms,omitempty"`
	Suspend         bool           `json:"suspend"`
	CancelRemaining bool           `json:"cancel_remaining,omitempty"`
}

// PersistAssetParams for persist_asset request
type PersistAssetParams struct {
	ExecutionID string           `json:"execution_id"`
	Paths       []string         `json:"paths,omitempty"`
	Metadata    map[string]any   `json:"metadata,omitempty"`
	Entries     map[string][]any `json:"entries,omitempty"` // Pre-resolved entries: {path: [blob_key, size, metadata]}
}

// PersistAssetResult is the response to persist_asset
type PersistAssetResult struct {
	Reference []any `json:"reference"`
}

// GetAssetParams for get_asset request
type GetAssetParams struct {
	ExecutionID string `json:"execution_id"`
	AssetID     string `json:"asset_id"`
}

// GetAssetResult is the response to get_asset
type GetAssetResult struct {
	Paths []string `json:"paths"`
}

// SuspendParams for suspend request
type SuspendParams struct {
	ExecutionID  string `json:"execution_id"`
	ExecuteAfter *int64 `json:"execute_after,omitempty"` // timestamp in ms
}

// CancelParams for cancel request
type CancelParams struct {
	ExecutionID string         `json:"execution_id"`
	Handles     []SelectHandle `json:"handles"`
}

// RegisterGroupParams for register_group notification
type RegisterGroupParams struct {
	ExecutionID string  `json:"execution_id"`
	GroupID     int     `json:"group_id"`
	Name        *string `json:"name,omitempty"`
}

// StreamRegisterParams for stream_register notification.
// Sequence is worker-assigned, monotonic per execution.
type StreamRegisterParams struct {
	ExecutionID string `json:"execution_id"`
	Sequence    int    `json:"sequence"`
}

// StreamAppendParams for stream_append notification.
// Position is worker-assigned, monotonic per stream.
type StreamAppendParams struct {
	ExecutionID string `json:"execution_id"`
	Sequence    int    `json:"sequence"`
	Position    int    `json:"position"`
	Value       *Value `json:"value"`
}

// StreamCloseParams for stream_close notification. Error is present only
// when the producer's generator raised an exception.
type StreamCloseParams struct {
	ExecutionID string            `json:"execution_id"`
	Sequence    int               `json:"sequence"`
	Error       *StreamCloseError `json:"error,omitempty"`
}

// StreamCloseError describes an error that terminated a stream.
type StreamCloseError struct {
	Type      string `json:"type"`
	Message   string `json:"message"`
	Traceback string `json:"traceback"`
}

// StreamSubscribeParams for stream_subscribe notification.
// Filter is one of nil, {"type": "slice", "start", "stop"},
// or {"type": "partition", "n", "i"}.
type StreamSubscribeParams struct {
	ExecutionID         string         `json:"execution_id"` // consumer
	SubscriptionID      int            `json:"subscription_id"`
	ProducerExecutionID string         `json:"producer_execution_id"`
	Sequence            int            `json:"sequence"`
	FromPosition        int            `json:"from_position"`
	Filter              map[string]any `json:"filter,omitempty"`
}

// StreamUnsubscribeParams for stream_unsubscribe notification.
type StreamUnsubscribeParams struct {
	ExecutionID    string `json:"execution_id"`
	SubscriptionID int    `json:"subscription_id"`
}

// StreamItemsParams for stream_items notification pushed CLI → adapter.
// Items are [[position, value], ...] where value is a wire Value.
type StreamItemsParams struct {
	ExecutionID    string `json:"execution_id"`
	SubscriptionID int    `json:"subscription_id"`
	Items          []any  `json:"items"`
}

// StreamClosedParams for stream_closed notification pushed CLI → adapter.
// Error is nil for clean close or a {type, message} dict for errored close.
type StreamClosedParams struct {
	ExecutionID    string         `json:"execution_id"`
	SubscriptionID int            `json:"subscription_id"`
	Error          map[string]any `json:"error,omitempty"`
}

// DownloadBlobParams for download_blob request
type DownloadBlobParams struct {
	ExecutionID string `json:"execution_id"`
	BlobKey     string `json:"blob_key"`
	TargetPath  string `json:"target_path"`
}

// UploadBlobParams for upload_blob request
type UploadBlobParams struct {
	ExecutionID string `json:"execution_id"`
	SourcePath  string `json:"source_path"`
}

// SubmitInputParams for submit_input request
type SubmitInputParams struct {
	ExecutionID  string              `json:"execution_id"`
	Template     string              `json:"template"`
	Placeholders map[string]*Value   `json:"placeholders,omitempty"`
	Schema       *string             `json:"schema,omitempty"` // JSON Schema as string
	Key          *string             `json:"key,omitempty"`
	Title        *string             `json:"title,omitempty"`
	Actions      []string            `json:"actions,omitempty"` // [respond_label, dismiss_label]
	Initial      any                 `json:"initial,omitempty"` // Plain JSON initial values
	Requires     map[string][]string `json:"requires,omitempty"`
}

// ParseMessage parses a JSON message from the executor
func ParseMessage(data []byte) (method string, id *int, params json.RawMessage, err error) {
	var msg struct {
		Method string          `json:"method"`
		ID     *int            `json:"id"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return "", nil, nil, err
	}
	return msg.Method, msg.ID, msg.Params, nil
}
