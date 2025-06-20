-- insighteye-query_v1.2.sql
-- Schema for InsightEye application, defining tables, indices, triggers, and maintenance functions.

-- Create extension for UUID support if not already created
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Start transaction to ensure atomicity
BEGIN;

-- Create function for updating timestamps
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create workspaces table to group users
CREATE TABLE IF NOT EXISTS workspaces (
    workspace_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT workspaces_name_unique UNIQUE (name)
) WITH (fillfactor=90);

-- Users table with workspace relationship
CREATE TABLE IF NOT EXISTS users (
    user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    count_of_camera INTEGER DEFAULT 5 NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    subscription_date TIMESTAMPTZ NOT NULL,
    is_subscribed BOOLEAN NOT NULL DEFAULT TRUE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_login TIMESTAMPTZ,
    role VARCHAR(15) NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin')), -- System-level roles (admin for global privileges)
    is_search BOOLEAN NOT NULL DEFAULT TRUE,
    is_prediction BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT users_username_unique UNIQUE (username),
    CONSTRAINT users_email_unique UNIQUE (email)
) WITH (fillfactor=90);

-- User-Workspace membership table
CREATE TABLE IF NOT EXISTS workspace_members (
    membership_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL DEFAULT 'member' CHECK (role IN ('member', 'admin', 'viewer')), -- Workspace-specific roles (admin for workspace privileges)
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT workspace_members_unique UNIQUE (workspace_id, user_id)
) WITH (fillfactor=90);

-- User accounts table for authentication
CREATE TABLE IF NOT EXISTS user_accounts (
    password_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    password_hash VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT user_accounts_user_id_unique UNIQUE (user_id)
) WITH (fillfactor=90);

-- Video stream table with workspace relationship
CREATE TABLE IF NOT EXISTS video_stream (
    stream_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    name VARCHAR(50) NOT NULL,
    path VARCHAR(255) NOT NULL,
    type VARCHAR(15) NOT NULL DEFAULT 'local' CHECK (type IN ('rtsp', 'http', 'local', 'other', 'video file')),
    status VARCHAR(10) NOT NULL DEFAULT 'inactive' CHECK (status IN ('active', 'inactive', 'error', 'processing')),
    is_streaming BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_activity TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
) WITH (fillfactor=85);

-- Parameter stream table with workspace relationship
CREATE TABLE IF NOT EXISTS param_stream (
    param_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    frame_delay REAL NOT NULL DEFAULT 0.0 CHECK (frame_delay >= 0),
    frame_skip SMALLINT NOT NULL DEFAULT 2 CHECK (frame_skip >= 0),
    conf REAL NOT NULL DEFAULT 0.4 CHECK (conf BETWEEN 0 AND 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT param_stream_workspace_unique UNIQUE (workspace_id)
) WITH (fillfactor=90);

-- Authentication and authorization tables
CREATE TABLE IF NOT EXISTS sessions (
    session_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    workspace_id UUID REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ NOT NULL,
    ip_address INET,
    user_agent VARCHAR(255)
) WITH (fillfactor=80);

-- User tokens with optimization
CREATE TABLE IF NOT EXISTS user_tokens (
    token_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    workspace_id UUID REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    access_token TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    access_expires_at TIMESTAMPTZ NOT NULL,
    refresh_expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
) WITH (fillfactor=80);

-- Token blacklist with optimization
CREATE TABLE IF NOT EXISTS token_blacklist (
    blacklist_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    token TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    blacklisted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reason VARCHAR(255)
) WITH (fillfactor=80);

-- Logs table with partitioning preparation
CREATE TABLE IF NOT EXISTS logs (
    log_id UUID DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    workspace_id UUID REFERENCES workspaces(workspace_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    action_type VARCHAR(50) NOT NULL,
    status VARCHAR(10) NOT NULL DEFAULT 'success' CHECK (status IN ('success', 'failure', 'warning', 'info', 'error')),
    ip_address INET,
    user_agent VARCHAR(255),
    content TEXT NOT NULL,
    PRIMARY KEY(log_id, created_at)
) WITH (fillfactor=100); -- Read-heavy, high fillfactor

-- Security events table with partitioning preparation
CREATE TABLE IF NOT EXISTS security_events (
    event_id UUID DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    workspace_id UUID REFERENCES workspaces(workspace_id) ON DELETE SET NULL,
    event_type VARCHAR(50) NOT NULL,
    severity VARCHAR(10) NOT NULL DEFAULT 'low' CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    ip_address INET,
    event_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(event_id, created_at)
) WITH (fillfactor=100); -- Read-heavy, high fillfactor

-- Create notifications table with workspace context
CREATE TABLE IF NOT EXISTS notifications (
    notification_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    stream_id UUID REFERENCES video_stream(stream_id) ON DELETE SET NULL,
    camera_name VARCHAR(100),
    status VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_read BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
) WITH (fillfactor=80);

-- OTP table with purpose column
CREATE TABLE IF NOT EXISTS otps (
    otp_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(100) NOT NULL,
    purpose VARCHAR(50) NOT NULL DEFAULT 'login' CHECK (purpose IN ('login', 'password_reset', 'email_verification')), -- Added purpose
    otp_hash VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    request_count INTEGER NOT NULL DEFAULT 0,
    last_request_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT otps_email_purpose_unique UNIQUE (email, purpose)
) WITH (fillfactor=90);

-- Create trigger for workspace timestamps
DROP TRIGGER IF EXISTS update_workspaces_timestamp ON workspaces;
CREATE TRIGGER update_workspaces_timestamp
BEFORE UPDATE ON workspaces
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION update_modified_column();

-- Create trigger for workspace members timestamps
DROP TRIGGER IF EXISTS update_workspace_members_timestamp ON workspace_members;
CREATE TRIGGER update_workspace_members_timestamp
BEFORE UPDATE ON workspace_members
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION update_modified_column();

-- Create triggers for automatic timestamp updates
DROP TRIGGER IF EXISTS update_user_accounts_timestamp ON user_accounts;
CREATE TRIGGER update_user_accounts_timestamp
BEFORE UPDATE ON user_accounts
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION update_modified_column();

DROP TRIGGER IF EXISTS update_video_stream_timestamp ON video_stream;
CREATE TRIGGER update_video_stream_timestamp
BEFORE UPDATE ON video_stream
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION update_modified_column();

DROP TRIGGER IF EXISTS update_param_stream_timestamp ON param_stream;
CREATE TRIGGER update_param_stream_timestamp
BEFORE UPDATE ON param_stream
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION update_modified_column();

DROP TRIGGER IF EXISTS update_notifications_timestamp ON notifications;
CREATE TRIGGER update_notifications_timestamp
BEFORE UPDATE ON notifications
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION update_modified_column();

-- Create minimal indices (additional indices are managed by database.py)
CREATE INDEX IF NOT EXISTS idx_workspaces_is_active ON workspaces(is_active);
CREATE INDEX IF NOT EXISTS idx_workspace_members_role ON workspace_members(role);
CREATE INDEX IF NOT EXISTS idx_workspace_members_ws_user_role ON workspace_members(workspace_id, user_id, role);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_last_login ON users(last_login DESC NULLS LAST) WHERE last_login IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_video_stream_workspace_id ON video_stream(workspace_id);
CREATE INDEX IF NOT EXISTS idx_video_stream_status ON video_stream(status);
CREATE INDEX IF NOT EXISTS idx_video_stream_ws_user ON video_stream(workspace_id, user_id);
CREATE INDEX IF NOT EXISTS idx_video_stream_active_workspace ON video_stream(workspace_id, status) WHERE is_streaming = TRUE;
CREATE INDEX IF NOT EXISTS idx_param_stream_workspace_id ON param_stream(workspace_id);
CREATE INDEX IF NOT EXISTS idx_sessions_workspace_id ON sessions(workspace_id);
CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions(user_id, expires_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_tokens_workspace_id ON user_tokens(workspace_id);
CREATE INDEX IF NOT EXISTS idx_user_tokens_refresh_token ON user_tokens(refresh_token) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_user_tokens_access_token ON user_tokens(access_token) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_user_tokens_active_user_refresh_exp ON user_tokens(user_id, refresh_expires_at DESC) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_token_blacklist_token ON token_blacklist(token);
CREATE INDEX IF NOT EXISTS idx_token_blacklist_user_id ON token_blacklist(user_id);
CREATE INDEX IF NOT EXISTS idx_logs_workspace_id ON logs(workspace_id);
CREATE INDEX IF NOT EXISTS idx_logs_action_type_created_at ON logs(action_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_logs_status_created_at ON logs(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_security_events_workspace_id ON security_events(workspace_id);
CREATE INDEX IF NOT EXISTS idx_security_events_event_type_created_at ON security_events(event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_security_events_severity_created_at ON security_events(severity, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_workspace_id ON notifications(workspace_id);
CREATE INDEX IF NOT EXISTS idx_notifications_stream_id ON notifications(stream_id);
CREATE INDEX IF NOT EXISTS idx_notifications_unread ON notifications(user_id, is_read) WHERE is_read = FALSE;
CREATE INDEX IF NOT EXISTS idx_notifications_ws_user_read ON notifications(workspace_id, user_id, is_read);

-- Add maintenance function to clean expired tokens and sessions
CREATE OR REPLACE FUNCTION cleanup_expired_data()
RETURNS void AS $$
BEGIN
    -- Delete expired sessions
    DELETE FROM sessions WHERE expires_at < CURRENT_TIMESTAMP;
    
    -- Delete expired tokens
    DELETE FROM user_tokens WHERE refresh_expires_at < CURRENT_TIMESTAMP;
    
    -- Delete expired blacklisted tokens
    DELETE FROM token_blacklist WHERE expires_at < CURRENT_TIMESTAMP;
    
    -- Delete expired OTPs
    DELETE FROM otps WHERE expires_at < CURRENT_TIMESTAMP;

    -- Log the cleanup
    INSERT INTO logs (log_id, user_id, action_type, status, content)
    VALUES (uuid_generate_v4(), NULL, 'system_cleanup', 'success', 'Cleaned up expired sessions, tokens, and OTPs');
END;
$$ LANGUAGE plpgsql;
-- Note: Schedule this function via a cron job or FastAPI background task (e.g., using APScheduler).

-- Commit transaction
COMMIT;
