-- Add up migration script here
CREATE TYPE job_status AS ENUM ('pending', 'running', 'completed', 'failed');

CREATE TABLE IF NOT EXISTS  jobs (
    id  UUID NOT NULL DEFAULT gen_random_uuid(),
    status job_status NOT NULL DEFAULT 'pending',

    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    
    args jsonb NOT NULL,
    
    PRIMARY KEY (id)
);
