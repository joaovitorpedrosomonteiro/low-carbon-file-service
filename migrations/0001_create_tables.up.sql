CREATE TABLE IF NOT EXISTS files (
    id VARCHAR(64) PRIMARY KEY,
    filename VARCHAR(512) NOT NULL,
    content_type VARCHAR(128) NOT NULL,
    size_bytes BIGINT NOT NULL,
    gcs_uri VARCHAR(1024) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'uploading',
    uploaded_by VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
CREATE INDEX IF NOT EXISTS idx_files_uploaded_by ON files(uploaded_by);
CREATE INDEX IF NOT EXISTS idx_files_created_at ON files(created_at);
