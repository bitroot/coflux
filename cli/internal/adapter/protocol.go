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
	Limit      *int   `json:"limit,omitempty"`
	DelayMinMs *int64 `json:"delay_min_ms,omitempty"`
	DelayMaxMs *int64 `json:"delay_max_ms,omitempty"`
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
	Target      string     `json:"target"`
	Arguments   []Argument `json:"arguments"`
}

// Argument represents a serialized argument value
type Argument struct {
	Type   string `json:"type"`   // "inline" or "file"
	Format string `json:"format"` // serialization format (e.g., "json", "pickle", "parquet", "pydantic")
	// For inline type
	Value any `json:"value,omitempty"`
	// For file type
	Path string `json:"path,omitempty"`
	// Optional metadata (e.g., model class name for pydantic)
	Metadata map[string]any `json:"metadata,omitempty"`
}

// Response is sent from CLI to executor in response to a request
type Response struct {
	ID     int        `json:"id"`
	Result any        `json:"result,omitempty"`
	Error  *ErrorInfo `json:"error,omitempty"`
}

// ErrorInfo describes an error in a response
type ErrorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ReadyMessage is sent by executor when it's ready for work
type ReadyMessage struct {
	Method string `json:"method"` // "ready"
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
	Type     string         `json:"type"`   // "inline" or "file"
	Format   string         `json:"format"` // serialization format
	Value    any            `json:"value,omitempty"`
	Path     string         `json:"path,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
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
	Target      string              `json:"target"`
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
}

// SubmitExecutionResult is the response to submit_execution
type SubmitExecutionResult struct {
	Reference []any `json:"reference"`
}

// ResolveReferenceParams for resolve_reference request
type ResolveReferenceParams struct {
	ExecutionID string `json:"execution_id"`
	Reference   []any  `json:"reference"`
}

// PersistAssetParams for persist_asset request
type PersistAssetParams struct {
	ExecutionID string         `json:"execution_id"`
	Paths       []string       `json:"paths"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// PersistAssetResult is the response to persist_asset
type PersistAssetResult struct {
	Reference []any `json:"reference"`
}

// GetAssetParams for get_asset request
type GetAssetParams struct {
	ExecutionID string `json:"execution_id"`
	Reference   []any  `json:"reference"`
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

// CancelExecutionParams for cancel_execution request
type CancelExecutionParams struct {
	ExecutionID     string `json:"execution_id"`
	TargetReference []any  `json:"target_reference"`
}

// RegisterGroupParams for register_group notification
type RegisterGroupParams struct {
	ExecutionID string  `json:"execution_id"`
	GroupID     int     `json:"group_id"`
	Name        *string `json:"name,omitempty"`
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
