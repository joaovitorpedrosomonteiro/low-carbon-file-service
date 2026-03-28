package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/domain/file"
	"github.com/joaovitorpedrosomonteiro/low-carbon-file-service/internal/domain/valueobject"
)

type FileRepository struct {
	pool *pgxpool.Pool
}

func NewFileRepository(pool *pgxpool.Pool) *FileRepository {
	return &FileRepository{pool: pool}
}

func (r *FileRepository) Save(ctx context.Context, f file.File) error {
	query := `INSERT INTO files (id, filename, content_type, size_bytes, gcs_uri, status, uploaded_by, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	_, err := r.pool.Exec(ctx, query,
		f.ID.String(),
		f.Filename,
		f.ContentType,
		f.SizeBytes,
		f.GCSUri.String(),
		string(f.Status),
		f.UploadedBy,
		f.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("insert file: %w", err)
	}
	return nil
}

func (r *FileRepository) FindByID(ctx context.Context, id uuid.UUID) (file.File, error) {
	query := `SELECT id, filename, content_type, size_bytes, gcs_uri, status, uploaded_by, created_at
		FROM files WHERE id = $1`

	var (
		idStr       string
		filename    string
		contentType string
		sizeBytes   int64
		gcsUriStr   string
		statusStr   string
		uploadedBy  string
		createdAt   string
	)

	err := r.pool.QueryRow(ctx, query, id.String()).Scan(
		&idStr,
		&filename,
		&contentType,
		&sizeBytes,
		&gcsUriStr,
		&statusStr,
		&uploadedBy,
		&createdAt,
	)

	if err != nil {
		if err.Error() == "no rows in result set" {
			return file.File{}, file.ErrFileNotFound
		}
		return file.File{}, fmt.Errorf("find file: %w", err)
	}

	parsedID, err := uuid.Parse(idStr)
	if err != nil {
		return file.File{}, fmt.Errorf("parse file ID: %w", err)
	}

	gcsUri, err := valueobject.NewGCSURI(gcsUriStr)
	if err != nil {
		return file.File{}, fmt.Errorf("parse GCS URI: %w", err)
	}

	_ = createdAt

	return file.File{
		ID:          parsedID,
		Filename:    filename,
		ContentType: contentType,
		SizeBytes:   sizeBytes,
		GCSUri:      gcsUri,
		Status:      file.FileStatus(statusStr),
		UploadedBy:  uploadedBy,
	}, nil
}

func (r *FileRepository) Update(ctx context.Context, f file.File) error {
	query := `UPDATE files SET filename = $1, content_type = $2, size_bytes = $3, gcs_uri = $4, status = $5, uploaded_by = $6
		WHERE id = $7`

	result, err := r.pool.Exec(ctx, query,
		f.Filename,
		f.ContentType,
		f.SizeBytes,
		f.GCSUri.String(),
		string(f.Status),
		f.UploadedBy,
		f.ID.String(),
	)

	if err != nil {
		return fmt.Errorf("update file: %w", err)
	}

	if result.RowsAffected() == 0 {
		return file.ErrFileNotFound
	}

	return nil
}

func (r *FileRepository) Delete(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM files WHERE id = $1`

	result, err := r.pool.Exec(ctx, query, id.String())
	if err != nil {
		return fmt.Errorf("delete file: %w", err)
	}

	if result.RowsAffected() == 0 {
		return file.ErrFileNotFound
	}

	return nil
}
