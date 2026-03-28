package valueobject

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidGCSURI     = errors.New("invalid GCS URI")
	ErrGCSURINotAllowed  = errors.New("GCS URI not in allowed bucket")
)

type GCSURI struct {
	value string
}

func NewGCSURI(raw string) (GCSURI, error) {
	if !strings.HasPrefix(raw, "gs://") {
		return GCSURI{}, fmt.Errorf("%w: must start with gs://", ErrInvalidGCSURI)
	}

	trimmed := strings.TrimPrefix(raw, "gs://")
	parts := strings.SplitN(trimmed, "/", 2)

	if parts[0] == "" {
		return GCSURI{}, fmt.Errorf("%w: missing bucket name", ErrInvalidGCSURI)
	}

	if len(parts) < 2 || parts[1] == "" {
		return GCSURI{}, fmt.Errorf("%w: missing object path", ErrInvalidGCSURI)
	}

	return GCSURI{value: raw}, nil
}

func NewGCSURIInBucket(raw string, allowedBucket string) (GCSURI, error) {
	uri, err := NewGCSURI(raw)
	if err != nil {
		return GCSURI{}, err
	}

	if uri.Bucket() != allowedBucket {
		return GCSURI{}, fmt.Errorf("%w: expected bucket %q, got %q", ErrGCSURINotAllowed, allowedBucket, uri.Bucket())
	}

	return uri, nil
}

func (u GCSURI) String() string {
	return u.value
}

func (u GCSURI) Bucket() string {
	trimmed := strings.TrimPrefix(u.value, "gs://")
	parts := strings.SplitN(trimmed, "/", 2)
	return parts[0]
}

func (u GCSURI) Object() string {
	trimmed := strings.TrimPrefix(u.value, "gs://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}
