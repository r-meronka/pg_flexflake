-- Snowflake ID Extension for PostgreSQL
-- Highly optimized via Active Code Generation (JIT-like behavior in PL/pgSQL)

-- State Table: Holds the last timestamp and sequence per worker.
-- UNLOGGED for maximum performance (skips WAL logging). 
-- FILLFACTOR=10 ensures HOT (Heap Only Tuple) updates to prevent index bloat.
CREATE UNLOGGED TABLE IF NOT EXISTS snowflake_state (
    datacenter_id  SMALLINT NOT NULL,
    worker_id      SMALLINT NOT NULL,
    last_ms        BIGINT   NOT NULL DEFAULT 0,
    sequence       INT      NOT NULL DEFAULT 0,
    PRIMARY KEY (datacenter_id, worker_id)
) WITH (fillfactor = 10);

-- Configuration Table: Stores the blueprint of the Snowflake ID structure.
CREATE TABLE IF NOT EXISTS snowflake_config (
    key            TEXT PRIMARY KEY,
    value          BIGINT NOT NULL,
    description    TEXT
);

-- Default configuration for standard Snowflake layout:
-- [1-bit sign][41-bit time][5-bit DC][5-bit worker][12-bit sequence]
INSERT INTO snowflake_config (key, value, description) VALUES
('bits_time', 41, 'Width of the timestamp field in bits'),
('bits_dc', 5, 'Width of the Datacenter ID field in bits'),
('bits_worker', 5, 'Width of the Worker ID field in bits'),
('bits_seq', 12, 'Width of the Sequence field in bits'),
('epoch', 1567987200000, 'Custom epoch start (ms). Default: 2019-09-09')
ON CONFLICT (key) DO NOTHING;

-- Helper: Get snowflake bits layout and masks
CREATE OR REPLACE FUNCTION snowflake_get_blueprints()
RETURNS TABLE(
    b_time INT, b_dc INT, b_worker INT, b_seq INT, 
    s_worker INT, s_dc INT, s_time INT,
    m_dc BIGINT, m_worker BIGINT, m_seq BIGINT,
    m_time BIGINT, v_epoch BIGINT
) AS $$
DECLARE
    cfg RECORD;
BEGIN
    SELECT 
        MAX(CASE WHEN key = 'bits_time'   THEN value END)::INT as b_time,
        MAX(CASE WHEN key = 'bits_dc'     THEN value END)::INT as b_dc,
        MAX(CASE WHEN key = 'bits_worker' THEN value END)::INT as b_worker,
        MAX(CASE WHEN key = 'bits_seq'    THEN value END)::INT as b_seq,
        MAX(CASE WHEN key = 'epoch'       THEN value END)::BIGINT as v_epoch
    INTO cfg FROM snowflake_config;

    RETURN QUERY SELECT 
        cfg.b_time, cfg.b_dc, cfg.b_worker, cfg.b_seq,
        cfg.b_seq,                               -- s_worker
        cfg.b_seq + cfg.b_worker,                -- s_dc
        cfg.b_seq + cfg.b_worker + cfg.b_dc,     -- s_time
        (1::BIGINT << cfg.b_dc) - 1,             -- m_dc
        (1::BIGINT << cfg.b_worker) - 1,         -- m_worker
        (1::BIGINT << cfg.b_seq) - 1,            -- m_seq
        (1::BIGINT << cfg.b_time) - 1,           -- m_time
        cfg.v_epoch;                             -- v_epoch
END;
$$ LANGUAGE plpgsql STABLE;

-- Helper: Reads Datacenter ID from config with fallback to default value
CREATE OR REPLACE FUNCTION snowflake_get_current_dc() RETURNS SMALLINT AS $$
BEGIN
    RETURN COALESCE(NULLIF(current_setting('snowflake.datacenter_id', true), ''), '1')::SMALLINT;
END;
$$ LANGUAGE plpgsql;

-- Helper: Reads Worker ID from config with fallback to default value
CREATE OR REPLACE FUNCTION snowflake_get_current_worker() RETURNS SMALLINT AS $$
BEGIN
    RETURN COALESCE(NULLIF(current_setting('snowflake.worker_id', true), ''), '1')::SMALLINT;
END;
$$ LANGUAGE plpgsql;

-- The Architect: Rebuilds the snowflake_nextval function with hardcoded constants.
CREATE OR REPLACE FUNCTION snowflake_rebuild()
RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    l RECORD;
    v_sql TEXT;
BEGIN
    -- Fetch Shifts and Masks from the unified layout helper
    SELECT * INTO l FROM snowflake_get_blueprints();

    -- CRITICAL: Lock and Truncate state table to maintain consistency with the new bit layout
    LOCK TABLE snowflake_state IN ACCESS EXCLUSIVE MODE;
    TRUNCATE snowflake_state;

    -- Warm up state table with current worker configuration
    INSERT INTO snowflake_state (datacenter_id, worker_id, last_ms, sequence)
    VALUES (snowflake_get_current_dc(), snowflake_get_current_worker(), 0, 0);

    -- Generate the main function using Dynamic SQL.
    -- Constants are injected as literals to ensure zero-latency retrieval.
    v_sql := format($template$
        CREATE OR REPLACE FUNCTION %I.snowflake_nextval(p_dc_id SMALLINT, p_w_id SMALLINT)
        RETURNS BIGINT LANGUAGE plpgsql AS $body$
        DECLARE
            -- Injected Static Constants (Hardcoded for maximum performance)
            EPOCH_OFFSET CONSTANT BIGINT  := %s;
            SHIFT_WORKER CONSTANT INT     := %s;
            SHIFT_DC     CONSTANT INT     := %s;
            SHIFT_TIME   CONSTANT INT     := %s;
            MAX_SEQ      CONSTANT INT     := %s;
            MASK_TIME    CONSTANT BIGINT  := %s;
            MASK_DC      CONSTANT BIGINT  := %s;
            MASK_WORKER  CONSTANT BIGINT  := %s;
            LOCK_SHIFT   CONSTANT INT     := %s;
            
            v_now       BIGINT; 
            v_last_ms   BIGINT; 
            v_seq       INT; 
            v_lock_key  BIGINT;
            v_drift_ms  BIGINT;
            v_max_drift CONSTANT INT := 1000; -- max 1 sec wait time
        BEGIN
            -- Transactional Advisory Lock per (DC, Worker) pair
            v_lock_key := (p_dc_id::BIGINT << LOCK_SHIFT) | p_w_id::BIGINT;
            PERFORM pg_advisory_xact_lock(v_lock_key);
            
            v_now := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT - EPOCH_OFFSET;
            
            -- Retrieve last state (Guaranteed to exist due to pre-initialization)
            SELECT last_ms, sequence INTO v_last_ms, v_seq 
            FROM snowflake_state
            WHERE datacenter_id = p_dc_id AND worker_id = p_w_id;

            -- Clock Drift Protection
            v_drift_ms := v_last_ms - v_now;
            IF v_drift_ms > 0 THEN
                IF v_drift_ms > v_max_drift THEN
                    RAISE EXCEPTION 'Clock drift too large (%% ms). Check system time!', v_drift_ms;
                END IF;

                RAISE NOTICE 'Clock drift detected! Waiting %% ms...', v_drift_ms;
                PERFORM pg_sleep(v_drift_ms / 1000.0);
                
                v_now := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT - EPOCH_OFFSET;
                IF v_now < v_last_ms THEN
                    RAISE EXCEPTION 'Clock still behind after sleep. Refusing to generate ID.';
                END IF;
            END IF;

            -- Sequence generation logic
            IF v_now = v_last_ms THEN
                v_seq := (v_seq + 1) & MAX_SEQ;
                IF v_seq = 0 THEN
                    -- Busy-wait until the next millisecond
                    WHILE v_now <= v_last_ms LOOP 
                        v_now := (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT - EPOCH_OFFSET; 
                    END LOOP;
                END IF;
            ELSE 
                v_seq := 0; 
            END IF;

            -- Persist state (HOT update)
            UPDATE snowflake_state SET last_ms = v_now, sequence = v_seq
            WHERE datacenter_id = p_dc_id AND worker_id = p_w_id;

            -- Assemble final BIGINT ID
            RETURN ((v_now & MASK_TIME) << SHIFT_TIME)
                 | ((p_dc_id::BIGINT & MASK_DC) << SHIFT_DC)
                 | ((p_w_id::BIGINT & MASK_WORKER) << SHIFT_WORKER)
                 | (v_seq::BIGINT & MAX_SEQ);
        END;
        $body$;
    $template$, 
    current_schema(), 
    l.v_epoch, l.s_worker, l.s_dc, l.s_time, l.m_seq, l.m_time, l.m_dc, l.m_worker, l.b_worker);

    EXECUTE v_sql;
    RETURN 'Snowflake Generator Rebuilt Successfully.';
END;
$$;

-- User Wrapper: Reads GUC settings and calls the optimized generator.
-- Uses EXECUTE to prevent plan caching issues after rebuild.
CREATE OR REPLACE FUNCTION snowflake_id()
RETURNS BIGINT LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    v_id BIGINT;
BEGIN
    EXECUTE format('SELECT snowflake_nextval(%L, %L)', 
        snowflake_get_current_dc(), 
        snowflake_get_current_worker()
    ) INTO v_id;
    
    RETURN v_id;
END;
$$;

-- Debugger: Retrieves the current configuration and validates bit counts.
CREATE OR REPLACE FUNCTION snowflake_get_config()
RETURNS TABLE(
    current_epoch BIGINT,
    bits_time INT, bits_dc INT, bits_worker INT, bits_seq INT,
    active_dc_id SMALLINT, active_worker_id SMALLINT,
    total_bits INT
) AS $$
DECLARE
    v_l RECORD;
    v_total INT;
BEGIN
    SELECT * INTO v_l FROM snowflake_get_blueprints();
    v_total := v_l.b_time + v_l.b_dc + v_l.b_worker + v_l.b_seq;

    IF v_total > 63 THEN
        RAISE WARNING 'CRITICAL BIT OVERFLOW: Total bits (%) exceeds 63!', v_total;
    END IF;

    RETURN QUERY SELECT 
        v_l.v_epoch,
        v_l.b_time, v_l.b_dc, v_l.b_worker, v_l.b_seq,
        snowflake_get_current_dc(),
        snowflake_get_current_worker(),
        v_total;
END;
$$ LANGUAGE plpgsql STABLE;

-- Debugger: Decodes a Snowflake ID using the current layout configuration.
CREATE OR REPLACE FUNCTION snowflake_parse(p_id BIGINT)
RETURNS TABLE(time_offset BIGINT, dc_id INT, worker_id INT, sequence INT) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        (p_id >> l.s_time),
        ((p_id >> l.s_dc) & l.m_dc)::INT,
        ((p_id >> l.s_worker) & l.m_worker)::INT,
        (p_id & l.m_seq)::INT
    FROM snowflake_get_blueprints() l;
END;
$$ LANGUAGE plpgsql STABLE;

-- Initial Build: Run the rebuilder once during installation.
SELECT snowflake_rebuild();