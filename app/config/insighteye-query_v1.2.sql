-- app/config/insighteye-query_v1.2.sql
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

-- Video stream table with workspace relationship and alert capabilities
CREATE TABLE IF NOT EXISTS video_stream (
    stream_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    name VARCHAR(50) NOT NULL,
    path VARCHAR(255) NOT NULL,
    type VARCHAR(15) NOT NULL DEFAULT 'local' CHECK (type IN ('rtsp', 'http', 'local', 'other', 'video file')),
    status VARCHAR(10) NOT NULL DEFAULT 'inactive' CHECK (status IN ('active', 'inactive', 'error', 'processing')),
    is_streaming BOOLEAN NOT NULL DEFAULT FALSE,
    location VARCHAR(100),
    area VARCHAR(100),
    building VARCHAR(100),
    floor_level VARCHAR(20),
    zone VARCHAR(50),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    count_threshold_greater INTEGER DEFAULT NULL,
    count_threshold_less INTEGER DEFAULT NULL,
    alert_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_activity TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
) WITH (fillfactor=85);

-- Create a locations table for standardized location management
CREATE TABLE IF NOT EXISTS camera_locations (
    location_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    location_name VARCHAR(100) NOT NULL,
    area_name VARCHAR(100),
    building VARCHAR(100),
    floor_level VARCHAR(20),
    zone VARCHAR(50),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT camera_locations_workspace_location_unique UNIQUE (workspace_id, location_name)
) WITH (fillfactor=90);

-- Parameter stream table with workspace relationship
CREATE TABLE IF NOT EXISTS param_stream (
    param_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    frame_delay REAL NOT NULL DEFAULT 0.0 CHECK (frame_delay >= 0),
    frame_skip SMALLINT NOT NULL DEFAULT 300 CHECK (frame_skip >= 0),
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

CREATE TABLE IF NOT EXISTS fire_detection_state (
    id SERIAL PRIMARY KEY,
    stream_id UUID NOT NULL REFERENCES video_stream(stream_id) ON DELETE CASCADE,
    fire_status VARCHAR(20) NOT NULL DEFAULT 'no detection',
    last_detection_time TIMESTAMPTZ,
    last_notification_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(stream_id)
);

CREATE TABLE IF NOT EXISTS people_count_alert_state (
    id SERIAL PRIMARY KEY,
    stream_id UUID NOT NULL REFERENCES video_stream(stream_id) ON DELETE CASCADE,
    last_notification_time TIMESTAMPTZ,
    last_count INTEGER,
    last_threshold_type VARCHAR(20), -- 'greater' or 'less'
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(stream_id)
);

CREATE INDEX IF NOT EXISTS idx_fire_detection_stream_id ON fire_detection_state(stream_id);
CREATE INDEX IF NOT EXISTS idx_fire_detection_last_notification ON fire_detection_state(last_notification_time);

CREATE INDEX IF NOT EXISTS idx_people_count_stream_id ON people_count_alert_state(stream_id);
CREATE INDEX IF NOT EXISTS idx_people_count_last_notification ON people_count_alert_state(last_notification_time);

-- Create triggers for automatic timestamp updates
DROP TRIGGER IF EXISTS update_workspaces_timestamp ON workspaces;
CREATE TRIGGER update_workspaces_timestamp
BEFORE UPDATE ON workspaces
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION update_modified_column();

DROP TRIGGER IF EXISTS update_workspace_members_timestamp ON workspace_members;
CREATE TRIGGER update_workspace_members_timestamp
BEFORE UPDATE ON workspace_members
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION update_modified_column();

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

DROP TRIGGER IF EXISTS update_camera_locations_timestamp ON camera_locations;
CREATE TRIGGER update_camera_locations_timestamp
BEFORE UPDATE ON camera_locations
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION update_modified_column();

-- Create indexes for optimal performance
CREATE INDEX IF NOT EXISTS idx_workspaces_is_active ON workspaces(is_active);
CREATE INDEX IF NOT EXISTS idx_workspace_members_role ON workspace_members(role);
CREATE INDEX IF NOT EXISTS idx_workspace_members_ws_user_role ON workspace_members(workspace_id, user_id, role);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_last_login ON users(last_login DESC NULLS LAST) WHERE last_login IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_video_stream_workspace_id ON video_stream(workspace_id);
CREATE INDEX IF NOT EXISTS idx_video_stream_status ON video_stream(status);
CREATE INDEX IF NOT EXISTS idx_video_stream_ws_user ON video_stream(workspace_id, user_id);
CREATE INDEX IF NOT EXISTS idx_video_stream_active_workspace ON video_stream(workspace_id, status) WHERE is_streaming = TRUE;
CREATE INDEX IF NOT EXISTS idx_video_stream_location ON video_stream(location);
CREATE INDEX IF NOT EXISTS idx_video_stream_area ON video_stream(area);
CREATE INDEX IF NOT EXISTS idx_video_stream_building ON video_stream(building);
CREATE INDEX IF NOT EXISTS idx_video_stream_zone ON video_stream(zone);
CREATE INDEX IF NOT EXISTS idx_video_stream_workspace_location ON video_stream(workspace_id, location);
CREATE INDEX IF NOT EXISTS idx_video_stream_workspace_area ON video_stream(workspace_id, area);
CREATE INDEX IF NOT EXISTS idx_video_stream_alert_enabled ON video_stream(alert_enabled) WHERE alert_enabled = TRUE;
CREATE INDEX IF NOT EXISTS idx_video_stream_alert_config ON video_stream(workspace_id, alert_enabled, count_threshold_greater, count_threshold_less);
CREATE INDEX IF NOT EXISTS idx_video_stream_location_alerts ON video_stream(workspace_id, location, alert_enabled) WHERE location IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_video_stream_alert_enabled_workspace ON video_stream(workspace_id, alert_enabled) WHERE alert_enabled = true;
CREATE INDEX IF NOT EXISTS idx_video_stream_thresholds ON video_stream(workspace_id, count_threshold_greater, count_threshold_less) WHERE count_threshold_greater IS NOT NULL OR count_threshold_less IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_camera_locations_workspace_id ON camera_locations(workspace_id);
CREATE INDEX IF NOT EXISTS idx_camera_locations_location_name ON camera_locations(location_name);
CREATE INDEX IF NOT EXISTS idx_camera_locations_area_name ON camera_locations(area_name);
CREATE INDEX IF NOT EXISTS idx_camera_locations_building ON camera_locations(building);
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

-- Create a function to validate alert threshold logic
CREATE OR REPLACE FUNCTION validate_alert_thresholds(
    p_greater_threshold INTEGER,
    p_less_threshold INTEGER
) RETURNS BOOLEAN AS $$
BEGIN
    -- If both thresholds are provided, ensure less < greater
    IF p_greater_threshold IS NOT NULL AND p_less_threshold IS NOT NULL THEN
        RETURN p_less_threshold < p_greater_threshold;
    END IF;
    
    -- If only one or neither threshold is provided, it's valid
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger to validate alert thresholds before insert/update
CREATE OR REPLACE FUNCTION validate_alert_thresholds_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT validate_alert_thresholds(NEW.count_threshold_greater, NEW.count_threshold_less) THEN
        RAISE EXCEPTION 'count_threshold_less must be less than count_threshold_greater';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop the trigger if it exists and create it
DROP TRIGGER IF EXISTS validate_alert_thresholds_trigger ON video_stream;
CREATE TRIGGER validate_alert_thresholds_trigger
    BEFORE INSERT OR UPDATE ON video_stream
    FOR EACH ROW
    EXECUTE FUNCTION validate_alert_thresholds_trigger();

-- Create a materialized view for alert statistics (refresh periodically)
CREATE MATERIALIZED VIEW IF NOT EXISTS alert_statistics AS
SELECT 
    vs.workspace_id,
    w.name as workspace_name,
    COUNT(*) as total_cameras,
    COUNT(CASE WHEN vs.alert_enabled = true THEN 1 END) as alert_enabled_cameras,
    COUNT(CASE WHEN vs.count_threshold_greater IS NOT NULL THEN 1 END) as cameras_with_greater_threshold,
    COUNT(CASE WHEN vs.count_threshold_less IS NOT NULL THEN 1 END) as cameras_with_less_threshold,
    COUNT(CASE WHEN vs.count_threshold_greater IS NOT NULL AND vs.count_threshold_less IS NOT NULL THEN 1 END) as cameras_with_both_thresholds,
    AVG(vs.count_threshold_greater) as avg_greater_threshold,
    AVG(vs.count_threshold_less) as avg_less_threshold,
    MIN(vs.count_threshold_greater) as min_greater_threshold,
    MAX(vs.count_threshold_greater) as max_greater_threshold,
    MIN(vs.count_threshold_less) as min_less_threshold,
    MAX(vs.count_threshold_less) as max_less_threshold,
    COUNT(CASE WHEN vs.status = 'active' AND vs.alert_enabled = true THEN 1 END) as active_alert_cameras,
    COUNT(CASE WHEN vs.status = 'error' AND vs.alert_enabled = true THEN 1 END) as error_alert_cameras,
    CURRENT_TIMESTAMP as last_updated
FROM video_stream vs
JOIN workspaces w ON vs.workspace_id = w.workspace_id
WHERE w.is_active = TRUE
GROUP BY vs.workspace_id, w.name;

-- Create index for alert_statistics after creating the materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_statistics_workspace ON alert_statistics(workspace_id);

-- Create a view for location-based camera statistics
CREATE OR REPLACE VIEW camera_location_stats AS
SELECT 
    vs.workspace_id,
    w.name as workspace_name,
    vs.location,
    vs.area,
    vs.building,
    vs.zone,
    COUNT(*) as total_cameras,
    COUNT(CASE WHEN vs.status = 'active' THEN 1 END) as active_cameras,
    COUNT(CASE WHEN vs.status = 'inactive' THEN 1 END) as inactive_cameras,
    COUNT(CASE WHEN vs.status = 'error' THEN 1 END) as error_cameras,
    COUNT(CASE WHEN vs.is_streaming = true THEN 1 END) as streaming_cameras,
    COUNT(CASE WHEN vs.alert_enabled = true THEN 1 END) as alert_enabled_cameras,
    ARRAY_AGG(vs.stream_id ORDER BY vs.name) as camera_ids,
    ARRAY_AGG(vs.name ORDER BY vs.name) as camera_names,
    ARRAY_AGG(
        CASE WHEN vs.alert_enabled = true THEN
            JSON_BUILD_OBJECT(
                'camera_id', vs.stream_id::text,
                'camera_name', vs.name,
                'count_threshold_greater', vs.count_threshold_greater,
                'count_threshold_less', vs.count_threshold_less
            )
        END
    ) FILTER (WHERE vs.alert_enabled = true) as alert_configurations
FROM video_stream vs
JOIN workspaces w ON vs.workspace_id = w.workspace_id
WHERE w.is_active = TRUE
GROUP BY vs.workspace_id, w.name, vs.location, vs.area, vs.building, vs.zone;

-- Create a view for cameras that need attention based on alert settings
CREATE OR REPLACE VIEW cameras_requiring_attention AS
SELECT 
    vs.stream_id,
    vs.workspace_id,
    vs.name,
    vs.location,
    vs.area,
    vs.building,
    vs.floor_level,
    vs.zone,
    vs.status,
    vs.is_streaming,
    vs.alert_enabled,
    vs.count_threshold_greater,
    vs.count_threshold_less,
    u.username as owner,
    CASE 
        WHEN vs.status = 'error' THEN 'Camera Error'
        WHEN vs.status = 'inactive' AND vs.alert_enabled = true THEN 'Inactive Camera with Alerts'
        WHEN vs.alert_enabled = true AND vs.count_threshold_greater IS NULL AND vs.count_threshold_less IS NULL THEN 'Alert Enabled but No Thresholds Set'
        ELSE 'OK'
    END as attention_reason
FROM video_stream vs
JOIN users u ON vs.user_id = u.user_id
WHERE vs.status = 'error' 
   OR (vs.status = 'inactive' AND vs.alert_enabled = true)
   OR (vs.alert_enabled = true AND vs.count_threshold_greater IS NULL AND vs.count_threshold_less IS NULL)
ORDER BY vs.workspace_id, vs.status, vs.name;

-- Create a function to refresh alert statistics
CREATE OR REPLACE FUNCTION refresh_alert_statistics()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW alert_statistics;
END;
$$ LANGUAGE plpgsql;

-- Create a function to get cameras with alert configurations
CREATE OR REPLACE FUNCTION get_cameras_with_alerts(p_workspace_id UUID)
RETURNS TABLE(
    camera_id UUID,
    camera_name VARCHAR(50),
    location VARCHAR(100),
    area VARCHAR(100),
    building VARCHAR(100),
    floor_level VARCHAR(20),
    zone VARCHAR(50),
    status VARCHAR(10),
    is_streaming BOOLEAN,
    alert_enabled BOOLEAN,
    count_threshold_greater INTEGER,
    count_threshold_less INTEGER,
    owner_username VARCHAR(50)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        vs.stream_id,
        vs.name,
        vs.location,
        vs.area,
        vs.building,
        vs.floor_level,
        vs.zone,
        vs.status,
        vs.is_streaming,
        vs.alert_enabled,
        vs.count_threshold_greater,
        vs.count_threshold_less,
        u.username
    FROM video_stream vs
    JOIN users u ON vs.user_id = u.user_id
    WHERE vs.workspace_id = p_workspace_id
    ORDER BY vs.alert_enabled DESC, vs.location, vs.name;
END;
$$ LANGUAGE plpgsql;

-- Create a function to get alert summary for a workspace
CREATE OR REPLACE FUNCTION get_workspace_alert_summary(p_workspace_id UUID)
RETURNS TABLE(
    total_cameras BIGINT,
    alert_enabled_cameras BIGINT,
    cameras_with_greater_threshold BIGINT,
    cameras_with_less_threshold BIGINT,
    cameras_with_both_thresholds BIGINT,
    avg_greater_threshold NUMERIC,
    avg_less_threshold NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*) as total_cameras,
        COUNT(CASE WHEN vs.alert_enabled = true THEN 1 END) as alert_enabled_cameras,
        COUNT(CASE WHEN vs.count_threshold_greater IS NOT NULL THEN 1 END) as cameras_with_greater_threshold,
        COUNT(CASE WHEN vs.count_threshold_less IS NOT NULL THEN 1 END) as cameras_with_less_threshold,
        COUNT(CASE WHEN vs.count_threshold_greater IS NOT NULL AND vs.count_threshold_less IS NOT NULL THEN 1 END) as cameras_with_both_thresholds,
        AVG(vs.count_threshold_greater) as avg_greater_threshold,
        AVG(vs.count_threshold_less) as avg_less_threshold
    FROM video_stream vs
    WHERE vs.workspace_id = p_workspace_id;
END;
$$ LANGUAGE plpgsql;

-- Function to get location hierarchy
CREATE OR REPLACE FUNCTION get_location_hierarchy(target_workspace_id UUID)
RETURNS TABLE(
    building VARCHAR(100),
    floor_level VARCHAR(20),
    zone VARCHAR(50),
    area VARCHAR(100),
    location VARCHAR(100),
    camera_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        vs.building,
        vs.floor_level,
        vs.zone,
        vs.area,
        vs.location,
        COUNT(*) as camera_count
    FROM video_stream vs
    WHERE vs.workspace_id = target_workspace_id
    AND (vs.building IS NOT NULL OR vs.floor_level IS NOT NULL OR vs.zone IS NOT NULL OR vs.area IS NOT NULL OR vs.location IS NOT NULL)
    GROUP BY vs.building, vs.floor_level, vs.zone, vs.area, vs.location
    ORDER BY vs.building, vs.floor_level, vs.zone, vs.area, vs.location;
END;
$$ LANGUAGE plpgsql;

-- Update the get_location_hierarchy function to include alert information
CREATE OR REPLACE FUNCTION get_location_hierarchy_with_alerts(p_workspace_id UUID)
RETURNS TABLE(
    building VARCHAR(100),
    floor_level VARCHAR(20),
    zone VARCHAR(50),
    area VARCHAR(100),
    location VARCHAR(100),
    camera_count BIGINT,
    alert_enabled_count BIGINT,
    active_count BIGINT,
    streaming_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COALESCE(vs.building, 'Unknown') as building,
        COALESCE(vs.floor_level, 'Unknown') as floor_level,
        COALESCE(vs.zone, 'Unknown') as zone,
        COALESCE(vs.area, 'Unknown') as area,
        COALESCE(vs.location, 'Unknown') as location,
        COUNT(*) as camera_count,
        COUNT(CASE WHEN vs.alert_enabled = true THEN 1 END) as alert_enabled_count,
        COUNT(CASE WHEN vs.status = 'active' THEN 1 END) as active_count,
        COUNT(CASE WHEN vs.is_streaming = true THEN 1 END) as streaming_count
    FROM video_stream vs
    WHERE vs.workspace_id = p_workspace_id
    GROUP BY COALESCE(vs.building, 'Unknown'), COALESCE(vs.floor_level, 'Unknown'), COALESCE(vs.zone, 'Unknown'), 
             COALESCE(vs.area, 'Unknown'), COALESCE(vs.location, 'Unknown')
    ORDER BY building, floor_level, zone, area, location;
END;
$$ LANGUAGE plpgsql;

-- Create a function to bulk update alert settings
CREATE OR REPLACE FUNCTION bulk_update_alert_settings(
    p_workspace_id UUID,
    p_camera_ids UUID[],
    p_count_threshold_greater INTEGER DEFAULT NULL,
    p_count_threshold_less INTEGER DEFAULT NULL,
    p_alert_enabled BOOLEAN DEFAULT NULL
) RETURNS TABLE(
    updated_count INTEGER,
    failed_count INTEGER,
    failed_camera_ids UUID[]
) AS $$
DECLARE
    v_updated_count INTEGER := 0;
    v_failed_count INTEGER := 0;
    v_failed_ids UUID[] := ARRAY[]::UUID[];
    v_camera_id UUID;
BEGIN
    -- Validate threshold logic if both are provided
    IF p_count_threshold_greater IS NOT NULL AND p_count_threshold_less IS NOT NULL THEN
        IF p_count_threshold_less >= p_count_threshold_greater THEN
            RAISE EXCEPTION 'count_threshold_less must be less than count_threshold_greater';
        END IF;
    END IF;
    
    -- Process each camera ID
    FOREACH v_camera_id IN ARRAY p_camera_ids
    LOOP
        BEGIN
            -- Check if camera exists in workspace
            IF NOT EXISTS (SELECT 1 FROM video_stream WHERE stream_id = v_camera_id AND workspace_id = p_workspace_id) THEN
                v_failed_count := v_failed_count + 1;
                v_failed_ids := array_append(v_failed_ids, v_camera_id);
                CONTINUE;
            END IF;
            
            -- Update the camera settings
            UPDATE video_stream 
            SET 
                count_threshold_greater = COALESCE(p_count_threshold_greater, count_threshold_greater),
                count_threshold_less = COALESCE(p_count_threshold_less, count_threshold_less),
                alert_enabled = COALESCE(p_alert_enabled, alert_enabled)
            WHERE stream_id = v_camera_id AND workspace_id = p_workspace_id;
            
            v_updated_count := v_updated_count + 1;
            
        EXCEPTION
            WHEN OTHERS THEN
                v_failed_count := v_failed_count + 1;
                v_failed_ids := array_append(v_failed_ids, v_camera_id);
        END;
    END LOOP;
    
    RETURN QUERY SELECT v_updated_count, v_failed_count, v_failed_ids;
END;
$$ LANGUAGE plpgsql;

-- Create a function to get cameras that may need alert configuration
CREATE OR REPLACE FUNCTION get_cameras_needing_alert_config(p_workspace_id UUID)
RETURNS TABLE(
    camera_id UUID,
    camera_name VARCHAR(50),
    location VARCHAR(100),
    area VARCHAR(100),
    building VARCHAR(100),
    floor_level VARCHAR(20),
    zone VARCHAR(50),
    status VARCHAR(10),
    is_streaming BOOLEAN,
    suggestion TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        vs.stream_id,
        vs.name,
        vs.location,
        vs.area,
        vs.building,
        vs.floor_level,
        vs.zone,
        vs.status,
        vs.is_streaming,
        CASE 
            WHEN vs.alert_enabled = false AND vs.status = 'active' THEN 'Consider enabling alerts for active camera'
            WHEN vs.alert_enabled = true AND vs.count_threshold_greater IS NULL AND vs.count_threshold_less IS NULL THEN 'Set alert thresholds for proper monitoring'
            WHEN vs.alert_enabled = true AND vs.status = 'inactive' THEN 'Camera is inactive but alerts are enabled'
            WHEN vs.status = 'error' THEN 'Camera has errors - check configuration'
            ELSE 'Configuration looks good'
        END as suggestion
    FROM video_stream vs
    WHERE vs.workspace_id = p_workspace_id
      AND (
          (vs.alert_enabled = false AND vs.status = 'active') OR
          (vs.alert_enabled = true AND vs.count_threshold_greater IS NULL AND vs.count_threshold_less IS NULL) OR
          (vs.alert_enabled = true AND vs.status = 'inactive') OR
          (vs.status = 'error')
      )
    ORDER BY vs.status, vs.name;
END;
$$ LANGUAGE plpgsql;

-- Add maintenance function to clean expired data
CREATE OR REPLACE FUNCTION cleanup_expired_data()
RETURNS VOID AS $$
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

-- Commit transaction
COMMIT;
