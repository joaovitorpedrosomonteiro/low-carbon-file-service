package query

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/domain/file"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/domain/valueobject"
)

type Storage interface {
	GenerateSignedURL(ctx context.Context, gcsUri string, expiry time.Duration) (string, error)
	ObjectExists(ctx context.Context, gcsUri string) (bool, error)
}

type FileResponse struct {
	ID          string `json:"id"`
	Filename    string `json:"filename"`
	ContentType string `json:"contentType"`
	SizeBytes   int64  `json:"sizeBytes"`
	GCSUri      string `json:"gcsUri"`
	Status      string `json:"status"`
	UploadedBy  string `json:"uploadedBy"`
	CreatedAt   string `json:"createdAt"`
	DownloadURL string `json:"downloadUrl,omitempty"`
}

type GetFileInput struct {
	FileID uuid.UUID
}

type GetFileOutput struct {
	File FileResponse
}

type GetFileHandler struct {
	repo    file.Repository
	storage Storage
}

func NewGetFileHandler(repo file.Repository, storage Storage) *GetFileHandler {
	return &GetFileHandler{
		repo:    repo,
		storage: storage,
	}
}

func (h *GetFileHandler) Handle(ctx context.Context, input GetFileInput) (GetFileOutput, error) {
	f, err := h.repo.FindByID(ctx, input.FileID)
	if err != nil {
		return GetFileOutput{}, err
	}

	resp := FileResponse{
		ID:          f.ID.String(),
		Filename:    f.Filename,
		ContentType: f.ContentType,
		SizeBytes:   f.SizeBytes,
		GCSUri:      f.GCSUri.String(),
		Status:      string(f.Status),
		UploadedBy:  f.UploadedBy,
		CreatedAt:   f.CreatedAt.Format(time.RFC3339),
	}

	if f.Status == file.StatusReady {
		signedURL, err := h.storage.GenerateSignedURL(ctx, f.GCSUri.String(), 1*time.Hour)
		if err == nil {
			resp.DownloadURL = signedURL
		}
	}

	return GetFileOutput{File: resp}, nil
}

type ValidateFileInput struct {
	FileID uuid.UUID
}

type ValidateFileOutput struct {
	Valid   bool   `json:"valid"`
	GCSUri  string `json:"gcsUri"`
	Exists  bool   `json:"exists"`
	Message string `json:"message"`
}

type ValidateFileHandler struct {
	repo    file.Repository
	storage Storage
}

func NewValidateFileHandler(repo file.Repository, storage Storage) *ValidateFileHandler {
	return &ValidateFileHandler{
		repo:    repo,
		storage: storage,
	}
}

func (h *ValidateFileHandler) Handle(ctx context.Context, input ValidateFileInput) (ValidateFileOutput, error) {
	f, err := h.repo.FindByID(ctx, input.FileID)
	if err != nil {
		return ValidateFileOutput{}, err
	}

	exists, err := h.storage.ObjectExists(ctx, f.GCSUri.String())
	if err != nil {
		return ValidateFileOutput{
			Valid:   false,
			GCSUri:  f.GCSUri.String(),
			Exists:  false,
			Message: fmt.Sprintf("error checking GCS object: %v", err),
		}, nil
	}

	valid := exists && f.Status == file.StatusReady
	message := "file is valid and accessible"
	if !exists {
		message = "GCS object not found"
	} else if f.Status != file.StatusReady {
		message = fmt.Sprintf("file status is %s, expected ready", f.Status)
	}

	return ValidateFileOutput{
		Valid:   valid,
		GCSUri:  f.GCSUri.String(),
		Exists:  exists,
		Message: message,
	}, nil
}

type GetDownloadURLInput struct {
	GCSURI valueobject.GCSURI
}

type GetDownloadURLOutput struct {
	URL string `json:"url"`
}

type GetDownloadURLHandler struct {
	storage Storage
}

func NewGetDownloadURLHandler(storage Storage) *GetDownloadURLHandler {
	return &GetDownloadURLHandler{storage: storage}
}

func (h *GetDownloadURLHandler) Handle(ctx context.Context, input GetDownloadURLInput) (GetDownloadURLOutput, error) {
	url, err := h.storage.GenerateSignedURL(ctx, input.GCSURI.String(), 1*time.Hour)
	if err != nil {
		return GetDownloadURLOutput{}, err
	}
	return GetDownloadURLOutput{URL: url}, nil
}
