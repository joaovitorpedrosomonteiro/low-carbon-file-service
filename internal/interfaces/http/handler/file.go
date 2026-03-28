package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/application/command"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/application/query"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/domain/file"
)

type FileHandler struct {
	uploadHandler          *command.UploadFileHandler
	deleteHandler          *command.DeleteFileHandler
	getFileHandler         *query.GetFileHandler
	validateHandler        *query.ValidateFileHandler
	initiateResumable      *command.InitiateResumableUploadHandler
	completeUploadHandler  *command.CompleteUploadHandler
}

func NewFileHandler(
	uploadHandler *command.UploadFileHandler,
	deleteHandler *command.DeleteFileHandler,
	getFileHandler *query.GetFileHandler,
	validateHandler *query.ValidateFileHandler,
	initiateResumable *command.InitiateResumableUploadHandler,
	completeUpload *command.CompleteUploadHandler,
) *FileHandler {
	return &FileHandler{
		uploadHandler:         uploadHandler,
		deleteHandler:         deleteHandler,
		getFileHandler:        getFileHandler,
		validateHandler:       validateHandler,
		initiateResumable:     initiateResumable,
		completeUploadHandler: completeUpload,
	}
}

type uploadResponse struct {
	FileID string `json:"fileId"`
	GCSUri string `json:"gcsUri"`
}

func (h *FileHandler) UploadFile(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Missing user context")
		return
	}

	if err := r.ParseMultipartForm(file.DirectUploadMax); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "Failed to parse multipart form")
		return
	}

	fileData, fileHeader, formErr := r.FormFile("file")
	if formErr != nil {
		writeError(w, http.StatusBadRequest, "MISSING_FILE", "No file provided in request")
		return
	}
	defer fileData.Close()

	contentType := fileHeader.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	objectPath := r.FormValue("objectPath")
	if objectPath == "" {
		writeError(w, http.StatusBadRequest, "MISSING_OBJECT_PATH", "objectPath is required")
		return
	}

	output, err := h.uploadHandler.Handle(r.Context(), command.UploadFileInput{
		Filename:    fileHeader.Filename,
		ContentType: contentType,
		SizeBytes:   fileHeader.Size,
		Data:        fileData,
		UploadedBy:  userID,
		ObjectPath:  objectPath,
	})

	if err != nil {
		handleDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, uploadResponse{
		FileID: output.FileID.String(),
		GCSUri: output.GCSUri,
	})
}

func (h *FileHandler) GetFile(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Missing user context")
		return
	}

	fileIDStr := r.PathValue("fileId")
	fileID, err := uuid.Parse(fileIDStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_FILE_ID", "Invalid file ID format")
		return
	}

	output, err := h.getFileHandler.Handle(r.Context(), query.GetFileInput{FileID: fileID})
	if err != nil {
		handleDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, output.File)
}

func (h *FileHandler) DeleteFile(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	userRole := r.Header.Get("X-User-Role")
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Missing user context")
		return
	}

	if userRole != "CompanyAdmin" && userRole != "SystemAdmin" {
		writeError(w, http.StatusForbidden, "FORBIDDEN", "Insufficient permissions to delete files")
		return
	}

	fileIDStr := r.PathValue("fileId")
	fileID, err := uuid.Parse(fileIDStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_FILE_ID", "Invalid file ID format")
		return
	}

	err = h.deleteHandler.Handle(r.Context(), command.DeleteFileInput{
		FileID:    fileID,
		DeletedBy: userID,
	})

	if err != nil {
		handleDomainError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *FileHandler) ValidateFile(w http.ResponseWriter, r *http.Request) {
	fileIDStr := r.PathValue("fileId")
	fileID, err := uuid.Parse(fileIDStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_FILE_ID", "Invalid file ID format")
		return
	}

	output, err := h.validateHandler.Handle(r.Context(), query.ValidateFileInput{FileID: fileID})
	if err != nil {
		handleDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, output)
}

type initiateResumableRequest struct {
	Filename    string `json:"filename"`
	ContentType string `json:"contentType"`
	SizeBytes   int64  `json:"sizeBytes"`
	Destination string `json:"destination"`
}

type initiateResumableResponse struct {
	FileID    string `json:"fileId"`
	UploadURI string `json:"uploadUri"`
}

func (h *FileHandler) InitiateResumableUpload(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Missing user context")
		return
	}

	var req initiateResumableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
		return
	}

	if req.Filename == "" {
		writeError(w, http.StatusBadRequest, "MISSING_FILENAME", "filename is required")
		return
	}
	if req.ContentType == "" {
		writeError(w, http.StatusBadRequest, "MISSING_CONTENT_TYPE", "contentType is required")
		return
	}
	if req.SizeBytes <= 0 {
		writeError(w, http.StatusBadRequest, "INVALID_SIZE", "sizeBytes must be greater than 0")
		return
	}
	if req.Destination == "" {
		writeError(w, http.StatusBadRequest, "MISSING_DESTINATION", "destination is required")
		return
	}

	output, err := h.initiateResumable.Handle(r.Context(), command.InitiateResumableUploadInput{
		Filename:    req.Filename,
		ContentType: req.ContentType,
		SizeBytes:   req.SizeBytes,
		UploadedBy:  userID,
		ObjectPath:  req.Destination,
	})

	if err != nil {
		handleDomainError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, initiateResumableResponse{
		FileID:    output.FileID.String(),
		UploadURI: output.UploadURI,
	})
}

type completeUploadRequest struct {
	FileID string `json:"fileId"`
}

func (h *FileHandler) CompleteUpload(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Missing user context")
		return
	}

	var req completeUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request body")
		return
	}

	fileID, err := uuid.Parse(req.FileID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_FILE_ID", "Invalid file ID format")
		return
	}

	err = h.completeUploadHandler.Handle(r.Context(), command.CompleteUploadInput{FileID: fileID})
	if err != nil {
		handleDomainError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handleDomainError(w http.ResponseWriter, err error) {
	if errors.Is(err, file.ErrFileNotFound) {
		writeError(w, http.StatusNotFound, "FILE_NOT_FOUND", "File not found")
		return
	}
	if errors.Is(err, file.ErrFileTooLarge) {
		writeError(w, http.StatusBadRequest, "FILE_TOO_LARGE", err.Error())
		return
	}
	if errors.Is(err, file.ErrInvalidStatusTransition) {
		writeError(w, http.StatusUnprocessableEntity, "INVALID_STATUS_TRANSITION", err.Error())
		return
	}
	if errors.Is(err, file.ErrInvalidContentType) {
		writeError(w, http.StatusBadRequest, "INVALID_CONTENT_TYPE", err.Error())
		return
	}

	writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error")
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(errorResponse{
		Error: errorBody{Code: code, Message: message},
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

type InitiateResumableRequest = initiateResumableRequest

type InitiateResumableResponse = initiateResumableResponse

type UploadResponse = uploadResponse

func splitPath(path string) (method, pattern string) {
	parts := strings.SplitN(path, " ", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", path
}

func (h *FileHandler) UploadResponse(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok")
}
