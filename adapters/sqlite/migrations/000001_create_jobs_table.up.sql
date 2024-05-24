CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    queue TEXT,
    priority INTEGER,
    status INTEGER,
    attempts INTEGER,
    payload TEXT,
    score INTEGER,
    available_at TIMESTAMP,
    created_at TIMESTAMP
);

CREATE INDEX idx_jobs_queue ON jobs (queue);
CREATE INDEX idx_jobs_count ON jobs (queue, status);
CREATE INDEX idx_jobs_score ON jobs (score);
CREATE INDEX idx_jobs_available_at ON jobs (available_at);