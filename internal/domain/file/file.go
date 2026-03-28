package file

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/domain/valueobject"
)

const (
	MaxFileSizeBytes int64 = 50 * 1024 * 1024
	DirectUploadMax  int64 = 10 * 1024 * 1024
)

type FileStatus string

const (
	StatusUploading FileStatus = "uploading"
	StatusQuarantine FileStatus = "quarantine"
	StatusReady     FileStatus = "ready"
	StatusExpired   FileStatus = "expired"
)

var (
	ErrFileNotFound          = errors.New("file not found")
	ErrFileTooLarge          = errors.New("file exceeds maximum size")
	ErrInvalidStatusTransition = errors.New("invalid file status transition")
	ErrInvalidContentType    = errors.New("unsupported content type")
	ErrFileNotReady          = errors.New("file is not ready")
)

var allowedMIMETypes = map[string]bool{
	"application/pdf": true,
	"text/csv":        true,
}

var validTransitions = map[FileStatus][]FileStatus{
	StatusUploading:  {StatusQuarantine, StatusExpired},
	StatusQuarantine: {StatusReady},
	StatusReady:      {},
	StatusExpired:    {},
}

type File struct {
	ID          uuid.UUID
	Filename    string
	ContentType string
	SizeBytes   int64
	GCSUri      valueobject.GCSURI
	Status      FileStatus
	UploadedBy  string
	CreatedAt   time.Time
}

func NewFile(id uuid.UUID, filename string, contentType string, sizeBytes int64, gcsUri valueobject.GCSURI, uploadedBy string) (File, error) {
	if sizeBytes > MaxFileSizeBytes {
		return File{}, fmt.Errorf("%w: %d bytes exceeds %d byte limit", ErrFileTooLarge, sizeBytes, MaxFileSizeBytes)
	}

	if !allowedMIMETypes[contentType] {
		return File{}, fmt.Errorf("%w: %s", ErrInvalidContentType, contentType)
	}

	if filename == "" {
		return File{}, errors.New("filename cannot be empty")
	}

	return File{
		ID:          id,
		Filename:    filename,
		ContentType: contentType,
		SizeBytes:   sizeBytes,
		GCSUri:      gcsUri,
		Status:      StatusUploading,
		UploadedBy:  uploadedBy,
		CreatedAt:   time.Now().UTC(),
	}, nil
}

func (f File) CanTransitionTo(next FileStatus) bool {
	allowed, exists := validTransitions[f.Status]
	if !exists {
		return false
	}
	for _, s := range allowed {
		if s == next {
			return true
		}
	}
	return false
}

func (f File) TransitionTo(next FileStatus) (File, error) {
	if !f.CanTransitionTo(next) {
		return File{}, fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStatusTransition, f.Status, next)
	}
	f.Status = next
	return f, nil
}

func IsAllowedMIME(contentType string) bool {
	return allowedMIMETypes[contentType]
}

func DirectUploadAllowed(sizeBytes int64) bool {
	return sizeBytes <= DirectUploadMax
}
