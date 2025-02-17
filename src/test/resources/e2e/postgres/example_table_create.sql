CREATE TABLE example_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    description TEXT,
    age INT,
    price DECIMAL(10,2),
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMPTZ,
    birth_date DATE,
    event_time TIME,
    metadata JSONB,
    ip_address INET,
    user_uuid UUID
);