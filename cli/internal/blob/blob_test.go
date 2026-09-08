package blob

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

// fakeStore records what it was asked, so a test can tell an upload that
// happened from one that was skipped.
type fakeStore struct {
	content     map[string][]byte
	existsErr   error
	existsCalls int
}

func (f *fakeStore) Get(key string) (io.ReadCloser, error) {
	content, ok := f.content[key]
	if !ok {
		return nil, nil
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (f *fakeStore) GetRange(key string, offset, length int64) (io.ReadCloser, error) {
	content, ok := f.content[key]
	if !ok {
		return nil, nil
	}
	if offset > int64(len(content)) {
		return nil, fmt.Errorf("range not satisfiable")
	}
	end := int64(len(content))
	if length >= 0 && offset+length < end {
		end = offset + length
	}
	return io.NopCloser(bytes.NewReader(content[offset:end])), nil
}

func (f *fakeStore) Exists(key string) (bool, error) {
	f.existsCalls++
	if f.existsErr != nil {
		return false, f.existsErr
	}
	_, ok := f.content[key]
	return ok, nil
}

func (f *fakeStore) Put(reader io.Reader) (string, error)    { return "", nil }
func (f *fakeStore) Upload(path string) (string, error)      { return "", nil }
func (f *fakeStore) Download(key, path string) (bool, error) { return false, nil }

func stored(key string, content string) *fakeStore {
	return &fakeStore{content: map[string][]byte{key: []byte(content)}}
}

func TestSkipUploadBelowThreshold(t *testing.T) {
	store := stored("abc", "hello")
	if skipUpload(store, "abc", existsCheckThreshold-1) {
		t.Fatal("expected small content to be uploaded without checking")
	}
	if store.existsCalls != 0 {
		t.Fatalf("expected no existence check, got %d", store.existsCalls)
	}
}

func TestSkipUploadWhenPresent(t *testing.T) {
	if !skipUpload(stored("abc", "hello"), "abc", existsCheckThreshold) {
		t.Fatal("expected stored content to be skipped")
	}
}

func TestSkipUploadWhenAbsent(t *testing.T) {
	if skipUpload(stored("abc", "hello"), "other", existsCheckThreshold) {
		t.Fatal("expected absent content to be uploaded")
	}
}

func TestSkipUploadOnCheckFailure(t *testing.T) {
	// The check is an optimisation, so a store that can't answer must not
	// stop the upload.
	store := stored("abc", "hello")
	store.existsErr = errors.New("nope")
	if skipUpload(store, "abc", existsCheckThreshold) {
		t.Fatal("expected a failed check to fall back to uploading")
	}
}

func TestRangeHeader(t *testing.T) {
	for _, c := range []struct {
		offset, length int64
		want           string
	}{
		{0, 16, "bytes=0-15"},
		{1000, 16, "bytes=1000-1015"},
		{1000, -1, "bytes=1000-"},
	} {
		if got := rangeHeader(c.offset, c.length); got != c.want {
			t.Errorf("rangeHeader(%d, %d) = %q, want %q", c.offset, c.length, got, c.want)
		}
	}
}

func TestManagerGetRangeFallsThroughStores(t *testing.T) {
	// The blob is missing from the first store, so the second answers.
	m := NewManager([]Store{stored("other", "xxx"), stored("abc", "0123456789")}, t.TempDir(), 200)

	reader, err := m.GetRange("abc", 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "3456" {
		t.Fatalf("got %q, want %q", got, "3456")
	}
}

func TestManagerGetRangeToEnd(t *testing.T) {
	m := NewManager([]Store{stored("abc", "0123456789")}, t.TempDir(), 200)

	reader, err := m.GetRange("abc", 6, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	got, _ := io.ReadAll(reader)
	if string(got) != "6789" {
		t.Fatalf("got %q, want %q", got, "6789")
	}
}

func TestManagerGetRangeNotFound(t *testing.T) {
	m := NewManager([]Store{stored("abc", "0123456789")}, t.TempDir(), 200)
	if _, err := m.GetRange("missing", 0, 4); err == nil {
		t.Fatal("expected an error for a blob no store holds")
	}
}
