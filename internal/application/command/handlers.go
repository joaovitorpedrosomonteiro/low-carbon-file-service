package command

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/domain/file"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/domain/valueobject"
)

type Storage interface {
	UploadObject(ctx context.Context, objectPath string, contentType string, data io.Reader) (string, error)
	DeleteObject(ctx context.Context, gcsUri string) error
	GenerateResumableUploadURL(ctx context.Context, objectPath string, contentType string, sizeBytes int64) (string, string, error)
	ObjectExists(ctx context.Context, gcsUri string) (bool, error)
}

type Publisher interface {
	Publish(ctx context.Context, eventType string, payload []byte) error
}

type UploadFileInput struct {
	Filename    string
	ContentType string
	SizeBytes   int64
	Data        io.Reader
	UploadedBy  string
	ObjectPath  string
}

type UploadFileOutput struct {
	FileID uuid.UUID
	GCSUri string
}

type UploadFileHandler struct {
	repo      file.Repository
	storage   Storage
	publisher Publisher
	bucket    string
}

func NewUploadFileHandler(repo file.Repository, storage Storage, publisher Publisher, bucket string) *UploadFileHandler {
	return &UploadFileHandler{
		repo:      repo,
		storage:   storage,
		publisher: publisher,
		bucket:    bucket,
	}
}

func (h *UploadFileHandler) Handle(ctx context.Context, input UploadFileInput) (UploadFileOutput, error) {
	if input.SizeBytes > file.MaxFileSizeBytes {
		return UploadFileOutput{}, fmt.Errorf("file size %d exceeds maximum %d", input.SizeBytes, file.MaxFileSizeBytes)
	}

	if !file.IsAllowedMIME(input.ContentType) {
		return UploadFileOutput{}, file.ErrInvalidContentType
	}

	fileID := uuid.New()
	gcsPath := input.ObjectPath

	gcsUriStr := fmt.Sprintf("gs://%s/%s", h.bucket, gcsPath)
	gcsUri, err := valueobject.NewGCSURIInBucket(gcsUriStr, h.bucket)
	if err != nil {
		return UploadFileOutput{}, fmt.Errorf("invalid GCS URI: %w", err)
	}

	f, err := file.NewFile(fileID, input.Filename, input.ContentType, input.SizeBytes, gcsUri, input.UploadedBy)
	if err != nil {
		return UploadFileOutput{}, err
	}

	uploadedGCSUri, err := h.storage.UploadObject(ctx, gcsPath, input.ContentType, input.Data)
	if err != nil {
		return UploadFileOutput{}, fmt.Errorf("upload to GCS failed: %w", err)
	}

	f, err = f.TransitionTo(file.StatusQuarantine)
	if err != nil {
		return UploadFileOutput{}, err
	}
	f, err = f.TransitionTo(file.StatusReady)
	if err != nil {
		return UploadFileOutput{}, err
	}

	if err := h.repo.Save(ctx, f); err != nil {
		return UploadFileOutput{}, fmt.Errorf("save file metadata: %w", err)
	}

	return UploadFileOutput{
		FileID: fileID,
		GCSUri: uploadedGCSUri,
	}, nil
}

type DeleteFileInput struct {
	FileID    uuid.UUID
	DeletedBy string
}

type DeleteFileHandler struct {
	repo    file.Repository
	storage Storage
}

func NewDeleteFileHandler(repo file.Repository, storage Storage) *DeleteFileHandler {
	return &DeleteFileHandler{
		repo:    repo,
		storage: storage,
	}
}

func (h *DeleteFileHandler) Handle(ctx context.Context, input DeleteFileInput) error {
	f, err := h.repo.FindByID(ctx, input.FileID)
	if err != nil {
		return err
	}

	if err := h.storage.DeleteObject(ctx, f.GCSUri.String()); err != nil {
		return fmt.Errorf("delete from GCS: %w", err)
	}

	return h.repo.Delete(ctx, input.FileID)
}

type InitiateResumableUploadInput struct {
	Filename    string
	ContentType string
	SizeBytes   int64
	UploadedBy  string
	ObjectPath  string
}

type InitiateResumableUploadOutput struct {
	FileID    uuid.UUID
	UploadURI string
}

type InitiateResumableUploadHandler struct {
	repo    file.Repository
	storage Storage
	bucket  string
}

func NewInitiateResumableUploadHandler(repo file.Repository, storage Storage, bucket string) *InitiateResumableUploadHandler {
	return &InitiateResumableUploadHandler{
		repo:    repo,
		storage: storage,
		bucket:  bucket,
	}
}

func (h *InitiateResumableUploadHandler) Handle(ctx context.Context, input InitiateResumableUploadInput) (InitiateResumableUploadOutput, error) {
	if input.SizeBytes > file.MaxFileSizeBytes {
		return InitiateResumableUploadOutput{}, fmt.Errorf("file size %d exceeds maximum %d", input.SizeBytes, file.MaxFileSizeBytes)
	}

	if !file.IsAllowedMIME(input.ContentType) {
		return InitiateResumableUploadOutput{}, file.ErrInvalidContentType
	}

	fileID := uuid.New()
	gcsPath := input.ObjectPath
	gcsUriStr := fmt.Sprintf("gs://%s/%s", h.bucket, gcsPath)

	gcsUri, err := valueobject.NewGCSURIInBucket(gcsUriStr, h.bucket)
	if err != nil {
		return InitiateResumableUploadOutput{}, err
	}

	f, err := file.NewFile(fileID, input.Filename, input.ContentType, input.SizeBytes, gcsUri, input.UploadedBy)
	if err != nil {
		return InitiateResumableUploadOutput{}, err
	}

	_, uploadURI, err := h.storage.GenerateResumableUploadURL(ctx, gcsPath, input.ContentType, input.SizeBytes)
	if err != nil {
		return InitiateResumableUploadOutput{}, fmt.Errorf("generate resumable upload URL: %w", err)
	}

	if err := h.repo.Save(ctx, f); err != nil {
		return InitiateResumableUploadOutput{}, fmt.Errorf("save file metadata: %w", err)
	}

	return InitiateResumableUploadOutput{
		FileID:    fileID,
		UploadURI: uploadURI,
	}, nil
}

type CompleteUploadInput struct {
	FileID uuid.UUID
}

type CompleteUploadHandler struct {
	repo      file.Repository
	publisher Publisher
}

func NewCompleteUploadHandler(repo file.Repository, publisher Publisher) *CompleteUploadHandler {
	return &CompleteUploadHandler{
		repo:      repo,
		publisher: publisher,
	}
}

func (h *CompleteUploadHandler) Handle(ctx context.Context, input CompleteUploadInput) error {
	f, err := h.repo.FindByID(ctx, input.FileID)
	if err != nil {
		return err
	}

	f, err = f.TransitionTo(file.StatusQuarantine)
	if err != nil {
		return err
	}

	if err := h.repo.Update(ctx, f); err != nil {
		return err
	}

	return nil
}

type MarkFileReadyInput struct {
	FileID uuid.UUID
}

type MarkFileReadyHandler struct {
	repo      file.Repository
	publisher Publisher
}

func NewMarkFileReadyHandler(repo file.Repository, publisher Publisher) *MarkFileReadyHandler {
	return &MarkFileReadyHandler{
		repo:      repo,
		publisher: publisher,
	}
}

func (h *MarkFileReadyHandler) Handle(ctx context.Context, input MarkFileReadyInput) error {
	f, err := h.repo.FindByID(ctx, input.FileID)
	if err != nil {
		return err
	}

	f, err = f.TransitionTo(file.StatusReady)
	if err != nil {
		return err
	}

	return h.repo.Update(ctx, f)
}

type ExpireUploadInput struct {
	FileID uuid.UUID
}

type ExpireUploadHandler struct {
	repo file.Repository
}

func NewExpireUploadHandler(repo file.Repository) *ExpireUploadHandler {
	return &ExpireUploadHandler{repo: repo}
}

func (h *ExpireUploadHandler) Handle(ctx context.Context, input ExpireUploadInput) error {
	f, err := h.repo.FindByID(ctx, input.FileID)
	if err != nil {
		return err
	}

	if time.Since(f.CreatedAt) < time.Hour {
		return fmt.Errorf("upload has not yet expired")
	}

	f, err = f.TransitionTo(file.StatusExpired)
	if err != nil {
		return err
	}

	return h.repo.Update(ctx, f)
}
