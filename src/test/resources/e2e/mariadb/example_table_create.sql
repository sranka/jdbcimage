CREATE TABLE example_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    description TEXT,
    age INT,
    price DECIMAL(10,2),
    is_active BOOLEAN,
    created_at DATETIME,
    updated_at DATETIME,
    birth_date DATE,
    event_time TIME,
    metadata JSON,
    ip_address VARCHAR(45),
    user_uuid CHAR(36)
);