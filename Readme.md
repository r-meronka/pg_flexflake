# pg_flexflake: PostgreSQL high-performance, flexible Snowflake ID generator

A high-performance, distributed unique ID generator for PostgreSQL, inspired by Twitter's Snowflake. This extension uses **Active Code Generation** to provide maximum performance by baking configuration constants directly into the generation logic.

Snowflake IDs offer a significant advantage over UUIDv7 by being half the size, occupying only 8 bytes (BIGINT) compared to the 16 bytes required for a 128-bit UUID. This storage efficiency leads to smaller indexes and reduced memory pressure, which drastically improves performance when managing billions of records. Unlike the rigid structure of UUIDv7, this Snowflake implementation allows for full bit-level customization, enabling developers to balance time precision against the number of supported workers. Furthermore, Snowflake IDs provide native chronological sortability (K-ordering) to prevent index fragmentation while being far more human-readable in URLs and logs than long hexadecimal strings. Combined with Active Code Generation, this solution ensures maximum throughput for high-load PostgreSQL environments where every byte and millisecond counts.

## Key Features

- **Zero Overhead**: Bitwise shifts and masks are hardcoded during reconfiguration.
- **Collision-Free**: Uses transactional advisory locks and sequence tracking.
- **Fully Configurable**: Supports custom Datacenter ID, Worker ID, and Epoch.
- **Flexible Layout**: Fully configurable bit widths for Time, Datacenter, and Worker IDs.
- **NTP Drift Protection**: Safely handles clock rollbacks.
- **Time-Ordered**: IDs are roughly sortable by creation time (K-ordered).

---

## Internal Architecture

### Active Code Generation (JIT-like)

Unlike other extensions that read configuration tables for every ID generated, this extension uses a **Meta-Generator** (`snowflake_rebuild`). It reads your configuration once and generates a new version of the snowflake_nextval function, embedding all bit-shifts and masks as **PL/pgSQL CONSTANTS**. This eliminates I/O overhead during ID generation.

### High-Concurrency Safety

    - **Advisory Locks:** Uses `pg_advisory_xact_lock` to ensure that a (Datacenter, Worker) pair never generates duplicate IDs, even under extreme parallel load.

    - **HOT Updates:** The state table uses a low FILLFACTOR (10), ensuring that updates to sequence numbers are Heap Only Tuple (HOT) updates, which significantly reduces index bloat and vacuum pressure.

    - **Unlogged State:** The sequence state is stored in an UNLOGGED table to skip WAL overhead, providing raw speed while maintaining transactional integrity within each ID request.

## Installation

### 1. Build and Install

Ensure you have `pg_config` in your PATH and the PostgreSQL development headers installed.

```bash
make
sudo make install
```
### 2. Enable in Database

Connect to your PostgreSQL instance and run:

```sql
CREATE EXTENSION snowflake;
```

_Note: This will automatically initialize the default configuration and build the generator function._

### Alternative: Manual Installation (No make required)

If you cannot or do not want to install PostgreSQL development headers, you can install the extension manually by copying the files directly to the PostgreSQL directory.
#### 1. Locate the Extension Directory

To ensure you are copying files to the correct path for your specific PostgreSQL version, use the pg_config utility (which is included with the standard database server installation):
Bash

Find the directory where .control and .sql files should live

```bash
pg_config --sharedir
```

The command will return a path like /usr/share/postgresql/16/. Your target directory for the extension files is the extension subdirectory within that path (e.g., /usr/share/postgresql/16/extension/).

#### 2. Copy the Files

Copy the extension files using sudo to ensure proper permissions:
Bash

_Replace {SHAREDIR} with the path obtained from pg_config._

```bash
sudo cp snowflake.control {SHAREDIR}/extension/
sudo cp snowflake--1.0.sql {SHAREDIR}/extension/
```

#### 3. Verify Permissions

Ensure the files are readable by the postgres user:

```bash
sudo chmod 644 {SHAREDIR}/extension/snowflake*
```

#### 4. Initialize in SQL

Log in to your database and activate the extension:

```sql
CREATE EXTENSION snowflake;
```
---

## Configuration

The extension utilizes a dual-layer configuration system to balance global structural integrity with local node flexibility:

    - **Relational Table** (`snowflake_config`): Stores the core blueprint, including bit-widths and the custom Epoch. This ensures that the ID structure remains consistent across the entire cluster. Changing these values requires a `snowflake_rebuild()` to "bake" the new constants into the generator function.

    - **Grand Unified Configuration (GUC):** Uses PostgreSQL system settings (e.g., snowflake.worker_id) to identify individual database instances.

### Why this separation?

This hybrid approach provides a Single Source of Truth for the ID format while allowing Local Autonomy for node identification. By keeping the structural blueprint in a table, we prevent accidental bit-layout mismatches between servers. Simultaneously, using GUC for Worker and Datacenter IDs allows you to use the same database image or template across multiple containers or VMs, simply by injecting different environment variables or configuration files without altering the internal table data.

### Default Values

By default, the extension uses the following 64-bit layout:

- **Time**: 41 bits (approx. 69 years)
    
- **Datacenter ID**: 5 bits (up to 32 DCs)
    
- **Worker ID**: 5 bits (up to 32 workers per DC)
    
- **Sequence**: 12 bits (4096 IDs per ms per worker)
    
- **Epoch**: `1567987200000` (2019-09-09 00:00:00 UTC)

### System-level Configuration (GUC)

To identify your specific server instance, add these lines to your `postgresql.conf`:

```
snowflake.datacenter_id = 1
snowflake.worker_id = 10
```

_Reload configuration (`SELECT pg_reload_conf();`) after changes._

---

## Reconfiguration Process

If you need to change the epoch or bit layout (e.g., more workers, fewer datacenters), follow this process.

### Required Permissions

- **Superuser** or **Database Owner** privileges are required to run the rebuild process, as it involves executing `CREATE OR REPLACE FUNCTION` and `TRUNCATE`.

### Steps:

1. Update the configuration table:
    
    ```sql
    UPDATE snowflake_config SET value = 8 WHERE key = 'bits_worker';
    UPDATE snowflake_config SET value = 9 WHERE key = 'bits_seq';
    ```
    
2. Trigger the meta-generator:
    
        
    ```sql
    SELECT snowflake_rebuild();
    ```

**WARNING**: Running `snowflake_rebuild()` will **TRUNCATE** the `snowflake_state` table to ensure bitwise consistency.

### Critical: Maintenance & Consistency

Running snowflake_rebuild() is an atomic operation, but it has important side effects:

    - **Table Locking:** It performs an **ACCESS EXCLUSIVE** lock on the state table. Active ID generation requests will pause and wait until the rebuild is finished.

    - **State Reset:** It **TRUNCATES** the snowflake_state table. This ensures that old sequence counters don't overflow into new bit boundaries.

    - **Plan Caching:** PostgreSQL caches function execution plans in long-lived sessions. After changing bit widths (e.g., bits_worker), you must restart application connections (or recycle your connection pool like PgBouncer) to ensure all sessions use the new bitwise constants.

#### Note on Session Persistence:

The extension uses EXECUTE in its wrapper to prevent plan caching issues, meaning that layout changes (via snowflake_rebuild) take effect immediately in all sessions. However, for maximum performance and to ensure clean state management, it is recommended to restart long-lived application connections or recycle your connection pool after a significant bit-layout reconfiguration.

---

## Diagnostics

### Verification

Before or after a rebuild, you can verify if the current session's GUC settings and bit layout are consistent:

```sql
SELECT * FROM snowflake_get_config();
```

This function will return the active configuration and issue a WARNING if the total bit count exceeds 63 (which would cause the sign bit to flip and generate negative IDs).

### ID Inspection

You can decompose any ID to see exactly when and where it was generated:

```sql
SELECT * FROM snowflake_parse(snowflake_id());
```

---

## Usage

### Generating IDs

Use the `snowflake_id()` function for primary keys or unique identifiers:

```sql
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY DEFAULT snowflake_id(),
    username TEXT NOT NULL
);

INSERT INTO users (username) VALUES ('johndoe');
```

### Session-level overrides

You can change the Worker ID for the current session without restarting the server:

```sql
SET snowflake.worker_id = 15;
SELECT snowflake_id();
```

### Parsing and Debugging

You can extract information from any generated ID using `snowflake_parse(BIGINT)`:

```sql
SELECT * FROM snowflake_parse(123456789012345678);
```

---

## Testing & Performance

To verify the uniqueness and performance, you can run a stress test:

```sql
-- Generate 1 million IDs and check for duplicates
SELECT count(id), count(distinct id) 
FROM (SELECT snowflake_id() as id FROM generate_series(1, 1000000)) s;
```

### Performance Note

The `snowflake_nextval` function is generated as a high-performance PL/pgSQL block. Because it relies on `UNLOGGED` tables and `CONSTANT` bit-shifts, it can reach throughput levels sufficient for most high-load applications without the need for external ID generation services.