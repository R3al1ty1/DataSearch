DO $$ BEGIN
    CREATE TYPE user_role_enum AS ENUM (
        'user',
        'admin'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255),
    full_name VARCHAR(255),

    role user_role_enum DEFAULT 'user' NOT NULL,

    is_active BOOLEAN DEFAULT true NOT NULL,
    is_email_verified BOOLEAN DEFAULT false NOT NULL,

    last_login_at TIMESTAMPTZ,

    oauth_provider VARCHAR(50),
    oauth_provider_id VARCHAR(255),

    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_is_active ON users(is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_oauth_provider_id
    ON users(oauth_provider, oauth_provider_id)
    WHERE oauth_provider IS NOT NULL AND oauth_provider_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS security_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    user_id UUID REFERENCES users(id) ON DELETE SET NULL,

    event_type VARCHAR(100) NOT NULL,
    ip_address VARCHAR(45),
    user_agent TEXT,

    details JSONB,

    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_security_events_user_id ON security_events(user_id);
CREATE INDEX IF NOT EXISTS idx_security_events_event_type ON security_events(event_type);
CREATE INDEX IF NOT EXISTS idx_security_events_created_at ON security_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_security_events_user_created ON security_events(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_security_events_type_created ON security_events(event_type, created_at DESC);

DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
