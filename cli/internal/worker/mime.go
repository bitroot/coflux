package worker

import "mime"

// Register MIME types for extensions that aren't in Go's built-in table (and
// that can't be relied on to be in the system table - slim container images
// typically don't ship /etc/mime.types), so that asset entries get consistent
// types regardless of where the worker runs.
func init() {
	types := map[string]string{
		".parquet": "application/vnd.apache.parquet",
		".csv":     "text/csv",
		".tsv":     "text/tab-separated-values",
		".jsonl":   "application/x-ndjson",
		".ndjson":  "application/x-ndjson",
		".md":      "text/markdown",
	}

	for extension, mimeType := range types {
		_ = mime.AddExtensionType(extension, mimeType)
	}
}
