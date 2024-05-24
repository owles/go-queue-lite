CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR(36) PRIMARY KEY,
    queue VARCHAR(255),
    priority INT,
    status INT,
    attempts INT,
    payload JSON,
    score BIGINT,
    available_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_jobs_queue ON jobs (queue);
CREATE INDEX idx_jobs_count ON jobs (queue, status);
CREATE INDEX idx_jobs_score ON jobs (score);
CREATE INDEX idx_jobs_available_at ON jobs (available_at);