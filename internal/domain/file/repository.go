package file

import (
	"context"

	"github.com/google/uuid"
)

type Repository interface {
	Save(ctx context.Context, file File) error
	FindByID(ctx context.Context, id uuid.UUID) (File, error)
	Update(ctx context.Context, file File) error
	Delete(ctx context.Context, id uuid.UUID) error
}
