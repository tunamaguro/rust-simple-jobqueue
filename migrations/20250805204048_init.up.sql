-- Add up migration script here
CREATE TABLE IF NOT EXISTS  job_waiting (
    id  UUID NOT NULL DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    
    args jsonb NOT NULL,
    
    PRIMARY KEY (id)
);