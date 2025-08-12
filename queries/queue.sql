-- name: PollJobs :many
UPDATE 
  jobs j
SET
  status = 'running',
  attempts = j.attempts + 1,
  lease_expires_at = clock_timestamp() + make_interval(secs := sqlc.arg(lease_seconds)::integer)
WHERE 
  id in (
    SELECT 
      id 
    FROM
      jobs
    WHERE 
      (status = 'pending' AND scheduled_at <= clock_timestamp())
      OR
      (status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= clock_timestamp())
    ORDER BY scheduled_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg(batch_size)
  )
RETURNING j.id, j.args;

-- name: HeartbeatJob :exec
UPDATE 
  jobs j
SET
  lease_expires_at = clock_timestamp() + make_interval(secs := sqlc.arg(lease_seconds)::integer)
WHERE
  id = $1;

-- name: CompleteJob :exec
UPDATE jobs
SET status = 'completed'
WHERE id = $1;

-- name: FailJob :exec
UPDATE jobs
SET status = 'failed'
WHERE id = $1;

-- name: RetryJob :exec
UPDATE jobs
SET status = 'pending',
    scheduled_at = NOW() + sqlc.arg(interval)::interval
WHERE id = $1;

-- name: InsertJob :exec
INSERT INTO jobs (args) VALUES (sqlc.arg(job_data));