package blob

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

// HTTPStore implements Store using HTTP GET/PUT
type HTTPStore struct {
	baseURL string
	client  *http.Client
}

// NewHTTPStore creates a new HTTP blob store
func NewHTTPStore(baseURL string) *HTTPStore {
	return &HTTPStore{
		baseURL: baseURL,
		client:  &http.Client{},
	}
}

// Get retrieves a blob by key
func (s *HTTPStore) Get(key string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/%s", s.baseURL, key)
	resp, err := s.client.Get(url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}
	return resp.Body, nil
}

// Put stores a blob and returns its key
func (s *HTTPStore) Put(reader io.Reader) (string, error) {
	// Read content to compute key and store
	content, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	key, err := ComputeKey(bytes.NewReader(content))
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf("%s/%s", s.baseURL, key)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(content))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	return key, nil
}

// Upload uploads a file and returns its key
func (s *HTTPStore) Upload(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	return s.Put(f)
}

// Download downloads a blob to a file
func (s *HTTPStore) Download(key, path string) (bool, error) {
	reader, err := s.Get(key)
	if err != nil {
		return false, err
	}
	if reader == nil {
		return false, nil
	}
	defer func() { _ = reader.Close() }()

	// Ensure directory exists
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return false, err
	}

	f, err := os.Create(path)
	if err != nil {
		return false, err
	}

	if _, err := io.Copy(f, reader); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return false, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return false, err
	}

	return true, nil
}
