-- name: PollJobs :many
WITH picked AS (
  SELECT id, scheduled_at
  FROM jobs
  WHERE status = 'pending'
  ORDER BY scheduled_at ASC
  FOR UPDATE SKIP LOCKED
  LIMIT $1
),
updated AS (
  UPDATE jobs j
  SET status = 'running'
  FROM picked
  WHERE j.id = picked.id
  RETURNING j.id, j.args, j.scheduled_at
)
SELECT id, args
FROM updated
ORDER BY scheduled_at ASC; 

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