-- Reference schema (also applied automatically from Lambda on first $connect via DDL).
-- Aurora PostgreSQL / RDS Data API.

CREATE TABLE IF NOT EXISTS connections (
  connection_id VARCHAR(128) PRIMARY KEY,
  session_id VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  partner_id VARCHAR(128),
  chat_id VARCHAR(128),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_connections_waiting ON connections (status, updated_at)
  WHERE status = 'waiting';

CREATE TABLE IF NOT EXISTS rate_limits (
  connection_id VARCHAR(128) PRIMARY KEY,
  window_start_ms BIGINT NOT NULL,
  msg_count INT NOT NULL
);
