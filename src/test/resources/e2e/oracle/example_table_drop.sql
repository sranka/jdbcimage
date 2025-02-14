BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE example_table';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN -- -942 means "table or view does not exist"
            RAISE;
        END IF;
END;
/