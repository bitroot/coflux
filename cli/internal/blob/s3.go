package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Store implements Store using AWS S3
type S3Store struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewS3Store creates a new S3 blob store
func NewS3Store(ctx context.Context, bucket, prefix, region string) (*S3Store, error) {
	// Build config options
	var optFns []func(*config.LoadOptions) error
	if region != "" {
		optFns = append(optFns, config.WithRegion(region))
	}

	cfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Normalize prefix (remove leading/trailing slashes)
	prefix = strings.Trim(prefix, "/")

	return &S3Store{
		client: s3.NewFromConfig(cfg),
		bucket: bucket,
		prefix: prefix,
	}, nil
}

// s3Key converts a blob key to an S3 key with sharding
// Format matches Python: {prefix}/{key[0:2]}/{key[2:4]}/{key[4:]}
func (s *S3Store) s3Key(key string) string {
	var shardedKey string
	if len(key) >= 4 {
		shardedKey = fmt.Sprintf("%s/%s/%s", key[:2], key[2:4], key[4:])
	} else {
		shardedKey = key
	}
	if s.prefix != "" {
		return s.prefix + "/" + shardedKey
	}
	return shardedKey
}

// Get retrieves a blob by key
func (s *S3Store) Get(key string) (io.ReadCloser, error) {
	ctx := context.Background()
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.s3Key(key)),
	})
	if err != nil {
		// Check for not found errors
		var noSuchKey *types.NoSuchKey
		var notFound *types.NotFound
		if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get S3 object: %w", err)
	}
	return output.Body, nil
}

// Put stores a blob and returns its key
func (s *S3Store) Put(reader io.Reader) (string, error) {
	// Read content to compute key
	content, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	key, err := ComputeKey(bytes.NewReader(content))
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.s3Key(key)),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload to S3: %w", err)
	}

	return key, nil
}

// Upload uploads a file and returns its key
func (s *S3Store) Upload(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	return s.Put(f)
}

// Download downloads a blob to a file
func (s *S3Store) Download(key, path string) (bool, error) {
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
