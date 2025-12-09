# UTP Dashboard – Snowflake Development Environment

This repository contains all development assets for the end-to-end data pipeline that powers the **UTP 2026 Dashboard**, including Snowflake SQL scripts, Python ingestion scripts, and environment setup files.

The goal is to maintain a reproducible, version-controlled workflow for ingestion (Bronze), transformations (Silver), and final models/views (Gold).

--------------------------------------------------------------------------------

## File Naming Convention

**Pattern:**

type_operation_type_name_stage.extension

### Where:

#### type
- dt – Dynamic Table
- t  – Table
- v  – View
- i  – Initialize
- ff – File Format

#### operation_type
- join
- filter
- init
- aggregate
- select
- load
- insert

#### name
A descriptive identifier for the file  
(e.g., project_details, carryovers, targets_scope)

#### stage
- bronze
- silver
- gold
- ref — reference tables

#### extension
- sql - SQL
- py  - Python

--------------------------------------------------------------------------------

## Architecture Overview

### Bronze Layer
Raw data ingestion from local CSVs.
Loaded via Python (ingest_carryovers_file_bronze.py).
Uses a reusable Python loader framework for all Bronze loads.
Standardizes data into Snowflake tables for downstream processing.

Typical example:
Weekly carryover CSV → BRONZE.CARRYOVERS
Exposed via a view like BRONZE.TARGETS_CARRYOVER_FILE for downstream use.

--------------------------------------------------------------------------------

### Silver Layer
Cleans, joins, and enriches Bronze datasets.
Uses modular SQL scripts, typically executed via:
- !source commands in SnowSQL / VS Code Snowflake extension
- Views and dynamic tables

Consistent naming convention: dt_..._silver.sql, v_..._silver.sql

Silver is where:
- Business rules are applied
- Exceptions are handled
- Joins between multiple Bronze sources occur
- Aggregations for reporting begin to appear

--------------------------------------------------------------------------------

### Gold Layer (Future)
Aggregated, dashboard-ready tables and views.
Intended to be consumed directly by Tableau / Power BI or reporting systems.
Usually updated via scheduled runs once Bronze/Silver pipelines are stable.

--------------------------------------------------------------------------------

## Setup Instructions

### Prerequisites
Install locally:
- Python 3.10+
- SnowSQL
- VS Code (optional)
- Snowflake Connector for Python:

```
pip install snowflake-connector-python
```

Optional (to reduce SSO pop-ups):
```
pip install "snowflake-connector-python[secure-local-storage]"
```

--------------------------------------------------------------------------------

## Authentication

This project uses externalbrowser authentication:
- No hard-coded passwords.
- Login happens via Azure AD or your organization's identity provider.
- When executing Python loaders:
  - A browser opens
  - You authenticate via SSO
  - Snowflake receives a token

If keyring is not installed, you may see warnings—this is safe.

--------------------------------------------------------------------------------

## Snowflake Environment Bootstrap

The scripts use a specific Role, Warehouse, Database and Schemas. Also need to make sure all the reference tables are initialized. To run all the scripts in your Snowflake environment there are 2 options:
 - Edit the Role, Warehouse, Database and Schema names in each script. (Lot of manual work)
 - Run the 'sf_init_env_bootstrap.sql' file
```
 snowsql -c dev -f .\sf_init_env_bootstrap.sq
```
where;
 - '-c' is the connection name. You might have different Snowflake accounts or configurations for development, production etc. You can edit the snowsql 'config' file with specifications like:
 ```
  [connections.my_example_connection]
  account = "your_account_name"
  user = "your_username"
  authenticator = "externalbrowser" # For SSO based Login
  role = "SYSADMIN"
  warehouse = "<none selected>"     # Optional if needs to be specified
  database = "<none selected>"      # Optional if needs to be specified
  schema = "<none selected>"        # Optional if needs to be specified
 ```

Note: The above config information is available in SnowSight Account Details under Settings. Just click on the User Icon in the bottom left. (UI as of 12th December 2025)

--------------------------------------------------------------------------------

## Weekly File Ingestion (Carryovers)

### Purpose
Load weekly carryover CSV (e.g., 2026 Carryover - TTI-v44.csv) into:

DASHBOARD.BRONZE.CARRYOVERS

This replaces manual Snowsight uploads.

### Files Involved (bronze/)
- file_loader_framework_bronze.py (framework)
- ingest_carryovers_file_bronze.py (carryover ingestion)
- t_init_targets_carryovers_bronze.sql (An Example)

--------------------------------------------------------------------------------

## Pre‑requisites in Snowflake
Run once:
- Path of init table sql file relative to ./bronze/.. Used as an argument to pass in the ingest file to init_sql_path. Eg: t_init_targets_carryovers_bronze.sql -> ./bronze/t_init.....sql
- Create file format

--------------------------------------------------------------------------------

## How the Bronze Loader Works

The loader steps:
1. Connect to Snowflake via externalbrowser
2. USE ROLE / WAREHOUSE / DATABASE / SCHEMA
3. TRUNCATE TABLE (optional)
4. PUT file into @%TABLE_NAME
5. COPY INTO TABLE with the file format
6. Log row count and add ingestion timestamp

This logic lives in file_loader_framework_bronze.py.

--------------------------------------------------------------------------------

## How to Run the Weekly Carryover Ingestion

1. Save latest carryover file in local system and note its path like:

C:\Users\<you>\Downloads\carryover_files\

2. Update local_file path inside ingest_carryovers_file_bronze.py:

```python
loader.load_file(
    local_file=r"C:\Users\...\2026 Carryover - TTI-v44.csv",  # Your file path
    table_name="CARRYOVERS",                                  # Table name in Snowflake to stage and copy into
    file_format="BRONZE.CARRYOVERS_CSV_FF",                   # File Format define in Snowflake
    truncate_before_load=True,                                # If you want to truncate (=TRUE) or append (=FALSE)
    on_error="ABORT_STATEMENT",
    init_sql_path=str(init_sql),                              # ensure table exists
)
```

3. Run:

```
cd snowflake-dev/bronze
python ingest_carryovers_file_bronze.py
```

4. Script behavior:
- Browser opens for SSO
- Checks if table exists in snowflake
- If table exists then moves ahead, if not then creates it from sql file path given in the argument
- Truncate the table eg: BRONZE.CARRYOVERS
- PUT file to @%Table_name eg: @%CARRYOVERS
- COPY INTO Table_name eg: BRONZE.CARRYOVERS
- Add a timestamp attribute to capture ingestion date and time
- Log “BRONZE.CARRYOVERS now has X rows”

5. Optional: Verify in Snowflake.

--------------------------------------------------------------------------------

## Running Transformation Pipeline (Bronze → Silver)
```
snowsql -f run_pipeline.sql
```

Or run !source scripts manually.

--------------------------------------------------------------------------------

## Bronze Loader Framework – Details

Located at:
```
bronze/file_loader_framework_bronze.py
```

### Components

SnowflakeConfig dataclass:
- account, user, warehouse, database, schema, role, authenticator

BronzeLoader class:
- _connect()
- _set_context()
- load_file():
  - Check If Exists
  - TRUNCATE
  - PUT file
  - COPY INTO table
  - Ingest Timestamp
  - Log row count

--------------------------------------------------------------------------------

## Reusing Framework for Another Bronze Table

To ingest another table/file in the future, create a new Python script and call:

```python
loader.load_file(
    local_file="path/to/new.csv",
    table_name="NEW_BRONZE_TABLE",
    file_format="BRONZE.NEW_FILE_FORMAT",
    init_sql_path="path/to/init_table.sql",
)
```

No need to rewrite the framework logic.

--------------------------------------------------------------------------------

## Maintainer

Sharvil Arjunwadkar \
Data Scientist - Texas A&M Transportation Institute
