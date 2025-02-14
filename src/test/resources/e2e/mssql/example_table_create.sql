CREATE TABLE example_table (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(100),
    description NVARCHAR(MAX),
    age INT,
    price DECIMAL(10,2),
    is_active BIT,
    created_at DATETIME,
    updated_at DATETIMEOFFSET,
    birth_date DATE,
    event_time TIME,
    metadata NVARCHAR(MAX),
    ip_address NVARCHAR(45),
    user_uuid UNIQUEIDENTIFIER
);