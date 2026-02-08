package api

import (
	"encoding/json"
	"fmt"
)

// Server message types (server -> client)
const (
	MsgTypeSession = 0 // [0, session_id, [execution_ids...]]
	MsgTypeCommand = 1 // [1, {command: string, params: []}]
	MsgTypeSuccess = 2 // [2, request_id, result]
	MsgTypeError   = 3 // [3, request_id, error]
)

// SessionMessage represents a session message from the server
type SessionMessage struct {
	SessionID    string
	ExecutionIDs []string
}

// Request represents an outgoing request to the server
type Request struct {
	Request string `json:"request"`
	Params  []any  `json:"params,omitempty"`
	ID      *int   `json:"id,omitempty"`
}

// CommandMessage represents a command from the server
type CommandMessage struct {
	Command string `json:"command"`
	Params  []any  `json:"params,omitempty"`
}

// ParseServerMessage parses a message from the server
func ParseServerMessage(data []byte) (msgType int, payload any, err error) {
	var msg []json.RawMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return 0, nil, fmt.Errorf("failed to parse server message: %w", err)
	}

	if len(msg) < 2 {
		return 0, nil, fmt.Errorf("invalid message format: too few elements")
	}

	if err := json.Unmarshal(msg[0], &msgType); err != nil {
		return 0, nil, fmt.Errorf("failed to parse message type: %w", err)
	}

	switch msgType {
	case MsgTypeSession:
		var sessionID string
		if err := json.Unmarshal(msg[1], &sessionID); err != nil {
			return 0, nil, fmt.Errorf("failed to parse session ID: %w", err)
		}
		var executionIDs []string
		if len(msg) >= 3 {
			if err := json.Unmarshal(msg[2], &executionIDs); err != nil {
				return 0, nil, fmt.Errorf("failed to parse execution IDs: %w", err)
			}
		}
		return msgType, SessionMessage{SessionID: sessionID, ExecutionIDs: executionIDs}, nil

	case MsgTypeCommand:
		var cmd CommandMessage
		if err := json.Unmarshal(msg[1], &cmd); err != nil {
			return 0, nil, fmt.Errorf("failed to parse command: %w", err)
		}
		return msgType, cmd, nil

	case MsgTypeSuccess:
		if len(msg) < 3 {
			return 0, nil, fmt.Errorf("invalid success message: missing fields")
		}
		var requestID int
		if err := json.Unmarshal(msg[1], &requestID); err != nil {
			return 0, nil, fmt.Errorf("failed to parse request ID: %w", err)
		}
		var result any
		if err := json.Unmarshal(msg[2], &result); err != nil {
			return 0, nil, fmt.Errorf("failed to parse result: %w", err)
		}
		return msgType, SuccessResponse{RequestID: requestID, Result: result}, nil

	case MsgTypeError:
		if len(msg) < 3 {
			return 0, nil, fmt.Errorf("invalid error message: missing fields")
		}
		var requestID int
		if err := json.Unmarshal(msg[1], &requestID); err != nil {
			return 0, nil, fmt.Errorf("failed to parse request ID: %w", err)
		}
		var errMsg any
		if err := json.Unmarshal(msg[2], &errMsg); err != nil {
			return 0, nil, fmt.Errorf("failed to parse error: %w", err)
		}
		return msgType, ErrorResponse{RequestID: requestID, Error: errMsg}, nil

	default:
		return 0, nil, fmt.Errorf("unknown message type: %d", msgType)
	}
}

// SuccessResponse represents a successful response from the server
type SuccessResponse struct {
	RequestID int
	Result    any
}

// ErrorResponse represents an error response from the server
type ErrorResponse struct {
	RequestID int
	Error     any
}

// Value types for serialized data
type ValueType string

const (
	ValueTypeRaw  ValueType = "raw"
	ValueTypeBlob ValueType = "blob"
)

// Value represents a serialized value from the server
type Value struct {
	Type ValueType
	// For raw values
	Content any
	// For blob values
	Key  string
	Size int64
	// References for both types
	References []Reference
}

// Reference types
type ReferenceType string

const (
	RefTypeExecution ReferenceType = "execution"
	RefTypeAsset     ReferenceType = "asset"
	RefTypeFragment  ReferenceType = "fragment"
)

// Reference represents a reference to an execution, asset, or fragment
type Reference struct {
	Type ReferenceType
	// For execution references
	ExecutionID string
	RunID       string
	StepID      string
	Attempt     int
	Module      string
	Target      string
	// For asset references
	AssetID    string
	Name       string
	TotalCount int
	TotalSize  int64
	// For fragment references
	Serializer string
	BlobKey    string
	Size       int64
	Metadata   map[string]any
}

// ParseValue parses a value from server format
func ParseValue(data []any) (*Value, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("empty value")
	}

	typeStr, ok := data[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid value type")
	}

	switch typeStr {
	case "raw":
		if len(data) < 3 {
			return nil, fmt.Errorf("invalid raw value: missing fields")
		}
		refs, err := parseReferences(data[2])
		if err != nil {
			return nil, err
		}
		return &Value{
			Type:       ValueTypeRaw,
			Content:    data[1],
			References: refs,
		}, nil

	case "blob":
		if len(data) < 4 {
			return nil, fmt.Errorf("invalid blob value: missing fields")
		}
		key, _ := data[1].(string)
		size, _ := data[2].(float64)
		refs, err := parseReferences(data[3])
		if err != nil {
			return nil, err
		}
		return &Value{
			Type:       ValueTypeBlob,
			Key:        key,
			Size:       int64(size),
			References: refs,
		}, nil

	default:
		return nil, fmt.Errorf("unknown value type: %s", typeStr)
	}
}

func parseReferences(data any) ([]Reference, error) {
	arr, ok := data.([]any)
	if !ok {
		return nil, fmt.Errorf("references must be an array")
	}

	refs := make([]Reference, 0, len(arr))
	for _, item := range arr {
		ref, err := ParseReference(item.([]any))
		if err != nil {
			return nil, err
		}
		refs = append(refs, *ref)
	}
	return refs, nil
}

// ParseReference parses a reference from server format
func ParseReference(data []any) (*Reference, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("invalid reference: too short")
	}

	typeStr, ok := data[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid reference type")
	}

	switch typeStr {
	case "execution":
		// ["execution", execution_id, run_id, step_id, attempt, module, target]
		if len(data) < 7 {
			return nil, fmt.Errorf("invalid execution reference")
		}
		attempt, _ := data[4].(float64)
		return &Reference{
			Type:        RefTypeExecution,
			ExecutionID: getString(data[1]),
			RunID:       getString(data[2]),
			StepID:      getString(data[3]),
			Attempt:     int(attempt),
			Module:      getString(data[5]),
			Target:      getString(data[6]),
		}, nil

	case "asset":
		// ["asset", external_id, name, total_count, total_size]
		if len(data) < 5 {
			return nil, fmt.Errorf("invalid asset reference")
		}
		totalCount, _ := data[3].(float64)
		totalSize, _ := data[4].(float64)
		return &Reference{
			Type:       RefTypeAsset,
			AssetID:    getString(data[1]),
			Name:       getString(data[2]),
			TotalCount: int(totalCount),
			TotalSize:  int64(totalSize),
		}, nil

	case "fragment":
		// ["fragment", serializer, blob_key, size, metadata]
		if len(data) < 5 {
			return nil, fmt.Errorf("invalid fragment reference")
		}
		size, _ := data[3].(float64)
		metadata, _ := data[4].(map[string]any)
		return &Reference{
			Type:       RefTypeFragment,
			Serializer: getString(data[1]),
			BlobKey:    getString(data[2]),
			Size:       int64(size),
			Metadata:   metadata,
		}, nil

	default:
		return nil, fmt.Errorf("unknown reference type: %s", typeStr)
	}
}

func getString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	default:
		return ""
	}
}
