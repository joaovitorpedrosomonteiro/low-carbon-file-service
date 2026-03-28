package gcs

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
)

type Client struct {
	client *storage.Client
	bucket string
}

func NewClient(ctx context.Context, bucket string) (*Client, error) {
	c, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create GCS client: %w", err)
	}

	return &Client{
		client: c,
		bucket: bucket,
	}, nil
}

func (c *Client) Close() error {
	return c.client.Close()
}

func (c *Client) UploadObject(ctx context.Context, objectPath string, contentType string, data io.Reader) (string, error) {
	obj := c.client.Bucket(c.bucket).Object(objectPath)
	writer := obj.NewWriter(ctx)
	writer.ContentType = contentType

	if _, err := io.Copy(writer, data); err != nil {
		writer.Close()
		return "", fmt.Errorf("write object: %w", err)
	}

	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("close writer: %w", err)
	}

	return fmt.Sprintf("gs://%s/%s", c.bucket, objectPath), nil
}

func (c *Client) DeleteObject(ctx context.Context, gcsUri string) error {
	objectPath, err := extractObjectPath(gcsUri, c.bucket)
	if err != nil {
		return err
	}

	obj := c.client.Bucket(c.bucket).Object(objectPath)
	if err := obj.Delete(ctx); err != nil {
		return fmt.Errorf("delete object %s: %w", objectPath, err)
	}

	return nil
}

func (c *Client) GenerateSignedURL(ctx context.Context, gcsUri string, expiry time.Duration) (string, error) {
	objectPath, err := extractObjectPath(gcsUri, c.bucket)
	if err != nil {
		return "", err
	}

	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "GET",
		Expires: time.Now().Add(expiry),
	}

	signedURL, err := c.client.Bucket(c.bucket).SignedURL(objectPath, opts)
	if err != nil {
		return "", fmt.Errorf("generate signed URL for %s: %w", objectPath, err)
	}

	return signedURL, nil
}

func (c *Client) GenerateResumableUploadURL(ctx context.Context, objectPath string, contentType string, sizeBytes int64) (string, string, error) {
	gcsUri := fmt.Sprintf("gs://%s/%s", c.bucket, objectPath)

	opts := &storage.SignedURLOptions{
		Scheme:      storage.SigningSchemeV4,
		Method:      "POST",
		ContentType: contentType,
		Expires:     time.Now().Add(1 * time.Hour),
		Headers: []string{
			fmt.Sprintf("x-goog-resumable:start"),
		},
		QueryParameters: url.Values{
			"uploadType": []string{"resumable"},
		},
	}

	uploadURL, err := c.client.Bucket(c.bucket).SignedURL(objectPath, opts)
	if err != nil {
		return "", "", fmt.Errorf("generate resumable upload URL: %w", err)
	}

	return gcsUri, uploadURL, nil
}

func (c *Client) ObjectExists(ctx context.Context, gcsUri string) (bool, error) {
	objectPath, err := extractObjectPath(gcsUri, c.bucket)
	if err != nil {
		return false, err
	}

	obj := c.client.Bucket(c.bucket).Object(objectPath)
	_, err = obj.Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		return false, fmt.Errorf("check object existence: %w", err)
	}

	return true, nil
}

func extractObjectPath(gcsUri string, expectedBucket string) (string, error) {
	if !strings.HasPrefix(gcsUri, "gs://") {
		return "", fmt.Errorf("invalid GCS URI: must start with gs://")
	}

	trimmed := strings.TrimPrefix(gcsUri, "gs://")
	parts := strings.SplitN(trimmed, "/", 2)

	if len(parts) < 2 || parts[1] == "" {
		return "", fmt.Errorf("invalid GCS URI: missing object path")
	}

	if parts[0] != expectedBucket {
		return "", fmt.Errorf("SSRF protection: URI bucket %q does not match expected %q", parts[0], expectedBucket)
	}

	return parts[1], nil
}

type MockClient struct {
	mu      sync.RWMutex
	objects map[string][]byte
	bucket  string
}

func NewMockClient(bucket string) *MockClient {
	return &MockClient{
		objects: make(map[string][]byte),
		bucket:  bucket,
	}
}

func (m *MockClient) UploadObject(ctx context.Context, objectPath string, contentType string, data io.Reader) (string, error) {
	content, err := io.ReadAll(data)
	if err != nil {
		return "", fmt.Errorf("read data: %w", err)
	}

	m.mu.Lock()
	m.objects[objectPath] = content
	m.mu.Unlock()

	return fmt.Sprintf("gs://%s/%s", m.bucket, objectPath), nil
}

func (m *MockClient) DeleteObject(ctx context.Context, gcsUri string) error {
	objectPath, err := extractObjectPath(gcsUri, m.bucket)
	if err != nil {
		return err
	}

	m.mu.Lock()
	delete(m.objects, objectPath)
	m.mu.Unlock()

	return nil
}

func (m *MockClient) GenerateSignedURL(ctx context.Context, gcsUri string, expiry time.Duration) (string, error) {
	objectPath, err := extractObjectPath(gcsUri, m.bucket)
	if err != nil {
		return "", err
	}

	m.mu.RLock()
	_, exists := m.objects[objectPath]
	m.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("object not found: %s", objectPath)
	}

	return fmt.Sprintf("http://localhost:8083/mock-download/%s", objectPath), nil
}

func (m *MockClient) GenerateResumableUploadURL(ctx context.Context, objectPath string, contentType string, sizeBytes int64) (string, string, error) {
	gcsUri := fmt.Sprintf("gs://%s/%s", m.bucket, objectPath)
	uploadURL := fmt.Sprintf("http://localhost:8083/mock-upload/%s", objectPath)
	return gcsUri, uploadURL, nil
}

func (m *MockClient) ObjectExists(ctx context.Context, gcsUri string) (bool, error) {
	objectPath, err := extractObjectPath(gcsUri, m.bucket)
	if err != nil {
		return false, err
	}

	m.mu.RLock()
	_, exists := m.objects[objectPath]
	m.mu.RUnlock()

	return exists, nil
}
