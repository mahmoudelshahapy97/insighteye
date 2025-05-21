ROLLBACK;

-- Create extension for UUID support if not already created
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Start transaction to ensure atomicity
BEGIN;

-- Create function for updating timestamps (optimized)
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
    is_active BOOLEAN NOT NULL DEFAULT TRUE
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
    role VARCHAR(15) NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin', 'moderator', 'workspace_admin')),
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
    role VARCHAR(20) NOT NULL DEFAULT 'member' CHECK (role IN ('member', 'admin', 'viewer')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT workspace_members_unique UNIQUE (workspace_id, user_id)
) WITH (fillfactor=90);

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
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE, -- Creator of the stream
    name VARCHAR(50) NOT NULL,     
    path VARCHAR(255) NOT NULL,
    type VARCHAR(10) NOT NULL DEFAULT 'local' CHECK (type IN ('rtsp', 'http', 'local', 'other', 'video file')),
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
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE, -- Creator of the parameters
    frame_delay REAL NOT NULL DEFAULT 5.5 CHECK (frame_delay >= 0),  
    frame_skip SMALLINT NOT NULL DEFAULT 5 CHECK (frame_skip >= 0),  
    conf REAL NOT NULL DEFAULT 0.5 CHECK (conf BETWEEN 0 AND 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT param_stream_workspace_unique UNIQUE (workspace_id)
) WITH (fillfactor=90);

-- Parameter stream camera table with workspace relationship
CREATE TABLE IF NOT EXISTS param_stream_camera (
    param_camera_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE, -- Creator of the camera parameters
    camera_id VARCHAR(100) NOT NULL,  
    frame_delay REAL NOT NULL DEFAULT 5.5 CHECK (frame_delay >= 0),  
    frame_skip SMALLINT NOT NULL DEFAULT 5 CHECK (frame_skip >= 0),  
    conf REAL NOT NULL DEFAULT 0.5 CHECK (conf BETWEEN 0 AND 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT param_stream_camera_unique UNIQUE (workspace_id, camera_id)
) WITH (fillfactor=90);

-- Authentication and authorization tables
CREATE TABLE IF NOT EXISTS sessions (
    session_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    workspace_id UUID REFERENCES workspaces(workspace_id) ON DELETE CASCADE, -- Current active workspace
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ NOT NULL,
    ip_address INET,  
    user_agent VARCHAR(255)  
) WITH (fillfactor=80);

-- User tokens with optimization
CREATE TABLE IF NOT EXISTS user_tokens (
    token_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    workspace_id UUID REFERENCES workspaces(workspace_id) ON DELETE CASCADE, -- Optional workspace context
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

-- Logs table with partitioning preparation and workspace context
CREATE TABLE IF NOT EXISTS logs (
    log_id UUID DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(user_id) ON DELETE SET NULL,
    workspace_id UUID REFERENCES workspaces(workspace_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    action_type VARCHAR(50) NOT NULL,
    status VARCHAR(10) NOT NULL DEFAULT 'success' CHECK (status IN ('success', 'failure', 'warning', 'info')),
    ip_address INET,  
    user_agent VARCHAR(255),  
    content TEXT NOT NULL,
    PRIMARY KEY(log_id, created_at)
);

-- Security events table with partitioning preparation and workspace context
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
);

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

DROP TRIGGER IF EXISTS update_param_stream_camera_timestamp ON param_stream_camera;
CREATE TRIGGER update_param_stream_camera_timestamp
BEFORE UPDATE ON param_stream_camera
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION update_modified_column();

DROP TRIGGER IF EXISTS update_notifications_timestamp ON notifications;
CREATE TRIGGER update_notifications_timestamp
BEFORE UPDATE ON notifications
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION update_modified_column();

-- Create optimized indexes for workspaces
CREATE INDEX IF NOT EXISTS idx_workspaces_is_active ON workspaces(is_active);

-- Create optimized indexes for workspace members
CREATE INDEX IF NOT EXISTS idx_workspace_members_workspace_id ON workspace_members(workspace_id);
CREATE INDEX IF NOT EXISTS idx_workspace_members_user_id ON workspace_members(user_id);
CREATE INDEX IF NOT EXISTS idx_workspace_members_role ON workspace_members(role);

-- Users indexes
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_last_login ON users(last_login) WHERE last_login IS NOT NULL;

-- User Passwords
CREATE INDEX IF NOT EXISTS idx_user_accounts_user_id ON user_accounts(user_id);

-- Video Stream
CREATE INDEX IF NOT EXISTS idx_video_stream_workspace_id ON video_stream(workspace_id);
CREATE INDEX IF NOT EXISTS idx_video_stream_user_id ON video_stream(user_id);
CREATE INDEX IF NOT EXISTS idx_video_stream_status ON video_stream(status);
CREATE INDEX IF NOT EXISTS idx_video_stream_active_workspace ON video_stream(workspace_id, status) 
    WHERE status = 'active';

-- Parameter stream
CREATE INDEX IF NOT EXISTS idx_param_stream_workspace_id ON param_stream(workspace_id);
CREATE INDEX IF NOT EXISTS idx_param_stream_user_id ON param_stream(user_id);

-- Parameter stream camera
CREATE INDEX IF NOT EXISTS idx_param_stream_camera_workspace_id ON param_stream_camera(workspace_id);
CREATE INDEX IF NOT EXISTS idx_param_stream_camera_user_id ON param_stream_camera(user_id);
CREATE INDEX IF NOT EXISTS idx_param_stream_camera_camera_id ON param_stream_camera(camera_id);

-- Sessions
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_workspace_id ON sessions(workspace_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions(user_id, expires_at);

-- User Tokens
CREATE INDEX IF NOT EXISTS idx_user_tokens_user_id ON user_tokens(user_id);
CREATE INDEX IF NOT EXISTS idx_user_tokens_workspace_id ON user_tokens(workspace_id);
CREATE INDEX IF NOT EXISTS idx_user_tokens_refresh_token ON user_tokens(refresh_token);
CREATE INDEX IF NOT EXISTS idx_user_tokens_active ON user_tokens(user_id) 
    WHERE is_active = TRUE;

-- Token Blacklist
CREATE INDEX IF NOT EXISTS idx_token_blacklist_expires_at ON token_blacklist(expires_at);
CREATE INDEX IF NOT EXISTS idx_token_blacklist_active ON token_blacklist(token, expires_at); 

-- Logs
CREATE INDEX IF NOT EXISTS idx_logs_workspace_id ON logs(workspace_id);
CREATE INDEX IF NOT EXISTS idx_logs_user_id ON logs(user_id);
CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at);

-- Security events
CREATE INDEX IF NOT EXISTS idx_security_events_workspace_id ON security_events(workspace_id);
CREATE INDEX IF NOT EXISTS idx_security_events_user_id ON security_events(user_id);
CREATE INDEX IF NOT EXISTS idx_security_events_created_at ON security_events(created_at);

-- Create indexes for notifications table
CREATE INDEX IF NOT EXISTS idx_notifications_workspace_id ON notifications(workspace_id);
CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id);
CREATE INDEX IF NOT EXISTS idx_notifications_stream_id ON notifications(stream_id);
CREATE INDEX IF NOT EXISTS idx_notifications_timestamp ON notifications(timestamp);
CREATE INDEX IF NOT EXISTS idx_notifications_unread ON notifications(user_id, is_read) WHERE is_read = FALSE;

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
    
    -- Log the cleanup
    INSERT INTO logs (user_id, action_type, status, content)
    VALUES (NULL, 'system_cleanup', 'success', 'Cleaned up expired sessions and tokens');
END;
$$ LANGUAGE plpgsql;

-- Create migration function to add workspace to existing data
CREATE OR REPLACE FUNCTION migrate_data_to_workspace_model()
RETURNS void AS $$
DECLARE
    default_workspace_id UUID;
    user_record RECORD;
BEGIN
    -- Create a default workspace
    INSERT INTO workspaces (name, description)
    VALUES ('Default Workspace', 'Automatically created during migration')
    RETURNING workspace_id INTO default_workspace_id;
    
    -- Add all existing users to the default workspace
    FOR user_record IN SELECT user_id FROM users LOOP
        INSERT INTO workspace_members (workspace_id, user_id, role)
        VALUES (default_workspace_id, user_record.user_id, 
               CASE WHEN user_record.role = 'admin' THEN 'admin' ELSE 'member' END);
    END LOOP;
    
    -- Update video_stream table
    ALTER TABLE video_stream ADD COLUMN workspace_id UUID REFERENCES workspaces(workspace_id);
    UPDATE video_stream SET workspace_id = default_workspace_id;
    ALTER TABLE video_stream ALTER COLUMN workspace_id SET NOT NULL;
    
    -- Update param_stream table
    ALTER TABLE param_stream ADD COLUMN workspace_id UUID REFERENCES workspaces(workspace_id);
    UPDATE param_stream SET workspace_id = default_workspace_id;
    ALTER TABLE param_stream ALTER COLUMN workspace_id SET NOT NULL;
    
    -- Update param_stream_camera table
    ALTER TABLE param_stream_camera ADD COLUMN workspace_id UUID REFERENCES workspaces(workspace_id);
    UPDATE param_stream_camera SET workspace_id = default_workspace_id;
    ALTER TABLE param_stream_camera ALTER COLUMN workspace_id SET NOT NULL;
    
    -- Update notifications table
    ALTER TABLE notifications ADD COLUMN workspace_id UUID REFERENCES workspaces(workspace_id);
    UPDATE notifications SET workspace_id = default_workspace_id;
    ALTER TABLE notifications ALTER COLUMN workspace_id SET NOT NULL;
    
    -- Note: This migration function is for existing data. For a fresh installation,
    -- the workspace_id columns should already be part of the table definitions.
END;
$$ LANGUAGE plpgsql;

-- Commit transaction
COMMIT;
