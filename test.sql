DO $$
BEGIN
    -- 1. Drop old table and create new one with structure matching the function result
    DROP TABLE IF EXISTS test_results;
    
    CREATE TEMP TABLE test_results AS 
    SELECT * FROM snowflake_parse(snowflake_id()) LIMIT 0;

    -- 2. Loop performing 1000 separate calls
    FOR i IN 1..1000 LOOP
        INSERT INTO test_results 
        SELECT * FROM snowflake_parse(snowflake_id());
    END LOOP;
END $$;

-- Display results after block completion
SELECT * FROM test_results;