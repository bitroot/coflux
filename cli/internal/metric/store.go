package metric

// Entry represents a single metric data point
type Entry struct {
	RunID       string  // Run ID (external)
	ExecutionID string  // Execution ID (external)
	WorkspaceID string  // Workspace ID (external)
	Key         string  // Metric key name
	Value       float64 // Metric value (the y value)
	At          float64 // X-axis value (seconds since execution start, or user-provided)
}

// Store is the interface for metric storage backends
type Store interface {
	// Record writes a batch of metric entries
	Record(entries []Entry) error
	// Flush sends any buffered entries
	Flush() error
	// Close flushes and closes the store
	Close() error
}

// NoopStore is a store that discards all metrics
type NoopStore struct{}

func (NoopStore) Record(entries []Entry) error { return nil }
func (NoopStore) Flush() error                 { return nil }
func (NoopStore) Close() error                 { return nil }
