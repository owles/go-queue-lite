DROP TABLE IF EXISTS jobs;

DROP INDEX IF EXISTS idx_jobs_queue ON jobs;
DROP INDEX IF EXISTS idx_jobs_count ON jobs;
DROP INDEX IF EXISTS idx_jobs_score ON jobs;
DROP INDEX IF EXISTS idx_jobs_available_at ON jobs;