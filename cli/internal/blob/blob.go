package blob

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Store is the interface for blob storage backends
type Store interface {
	// Get retrieves a blob by key, returns nil if not found
	Get(key string) (io.ReadCloser, error)
	// GetRange retrieves a byte range of a blob, returns nil if not found.
	// A negative length reads to the end of the blob.
	GetRange(key string, offset, length int64) (io.ReadCloser, error)
	// Exists reports whether a blob is already stored
	Exists(key string) (bool, error)
	// Put stores a blob and returns its key (content-addressed)
	Put(reader io.Reader) (string, error)
	// Upload uploads a file and returns its key
	Upload(path string) (string, error)
	// Download downloads a blob to a file, returns true if successful
	Download(key, path string) (bool, error)
}

// Blobs at or above this size are checked for existence before being
// uploaded.
const existsCheckThreshold = 1 << 20 // 1 MiB

// skipUpload reports whether content with this key is already stored, and
// so needn't be sent again. Content addressing makes re-uploading it
// redundant, but asking costs a round trip, so it's only worth it once the
// content is big enough that re-sending would cost more than asking.
//
// A failed check reports "not stored": this is only an optimisation, and
// uploading something that's already there is always safe.
func skipUpload(store Store, key string, size int) bool {
	if size < existsCheckThreshold {
		return false
	}
	exists, err := store.Exists(key)
	return err == nil && exists
}

// rangeHeader formats an HTTP byte range. A negative length means "to the
// end of the blob".
func rangeHeader(offset, length int64) string {
	if length < 0 {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
}

// Manager manages multiple blob stores with fallback
type Manager struct {
	stores    []Store
	cacheDir  string
	threshold int // bytes; values larger than this are stored as blobs
}

// NewManager creates a new blob manager
func NewManager(stores []Store, cacheDir string, threshold int) *Manager {
	return &Manager{
		stores:    stores,
		cacheDir:  cacheDir,
		threshold: threshold,
	}
}

// Get retrieves a blob from any store
func (m *Manager) Get(key string) (io.ReadCloser, error) {
	for _, store := range m.stores {
		reader, err := store.Get(key)
		if err != nil {
			return nil, err
		}
		if reader != nil {
			return reader, nil
		}
	}
	return nil, fmt.Errorf("blob not found: %s", key)
}

// GetRange retrieves a byte range of a blob from any store. A negative
// length reads to the end of the blob.
func (m *Manager) GetRange(key string, offset, length int64) (io.ReadCloser, error) {
	// Deliberately not served from the cache: it holds whole blobs, and
	// Download treats the file merely existing as a complete one, so a
	// range must never be written there.
	for _, store := range m.stores {
		reader, err := store.GetRange(key, offset, length)
		if err != nil {
			return nil, err
		}
		if reader != nil {
			return reader, nil
		}
	}
	return nil, fmt.Errorf("blob not found: %s", key)
}

// Put stores a blob in the first store
func (m *Manager) Put(reader io.Reader) (string, error) {
	if len(m.stores) == 0 {
		return "", fmt.Errorf("no blob stores configured")
	}
	return m.stores[0].Put(reader)
}

// Upload uploads a file to the first store
func (m *Manager) Upload(path string) (string, error) {
	if len(m.stores) == 0 {
		return "", fmt.Errorf("no blob stores configured")
	}
	return m.stores[0].Upload(path)
}

// UploadData uploads byte data to the first store
func (m *Manager) UploadData(data []byte) (string, error) {
	if len(m.stores) == 0 {
		return "", fmt.Errorf("no blob stores configured")
	}
	return m.stores[0].Put(bytes.NewReader(data))
}

// Download downloads a blob to the cache directory
func (m *Manager) Download(key string) (string, error) {
	// Check if already cached
	cachePath := m.CachePath(key)
	if _, err := os.Stat(cachePath); err == nil {
		return cachePath, nil
	}

	// Try each store
	for _, store := range m.stores {
		ok, err := store.Download(key, cachePath)
		if err != nil {
			return "", err
		}
		if ok {
			return cachePath, nil
		}
	}

	return "", fmt.Errorf("blob not found: %s", key)
}

// DownloadTo downloads a blob to a specific path
func (m *Manager) DownloadTo(key, targetPath string) error {
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return err
	}

	// Try each store
	for _, store := range m.stores {
		ok, err := store.Download(key, targetPath)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}

	return fmt.Errorf("blob not found: %s", key)
}

// DownloadRangeTo downloads a byte range of a blob to a specific path. A
// negative length reads to the end of the blob.
func (m *Manager) DownloadRangeTo(key, targetPath string, offset, length int64) error {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return err
	}

	f, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if length == 0 {
		return nil
	}

	reader, err := m.GetRange(key, offset, length)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	_, err = io.Copy(f, reader)
	return err
}

// CachePath returns the cache path for a blob key
func (m *Manager) CachePath(key string) string {
	// Use first 4 chars for directory sharding
	dir := filepath.Join(m.cacheDir, key[:2], key[2:4])
	return filepath.Join(dir, key)
}

// EnsureCacheDir creates the cache directory if it doesn't exist
func (m *Manager) EnsureCacheDir() error {
	return os.MkdirAll(m.cacheDir, 0755)
}

// Threshold returns the blob threshold in bytes
func (m *Manager) Threshold() int {
	return m.threshold
}

// ComputeKey computes the SHA256 key for content
func ComputeKey(reader io.Reader) (string, error) {
	hash := sha256.New()
	if _, err := io.Copy(hash, reader); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ComputeKeyFromFile computes the SHA256 key for a file
func ComputeKeyFromFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	return ComputeKey(f)
}
