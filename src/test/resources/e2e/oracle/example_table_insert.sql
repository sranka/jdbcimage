INSERT INTO example_table (
    name, description, age, price, is_active, created_at, updated_at,
    birth_date, event_time, metadata, ip_address, user_uuid
) VALUES (
     'John Doe',
     'This is a sample description.',
     30,
     99.99,
     'Y',
     TIMESTAMP '2024-01-01 12:00:00',
     TIMESTAMP '2024-01-01 12:00:00 +00:00',
     DATE '1993-05-20',
     TIMESTAMP '2024-01-01 14:30:00',
     '{"key": "value"}',
     '192.168.1.1',
     HEXTORAW('550E8400E29B41D4A716446655440000')
 );