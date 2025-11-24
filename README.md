# UTP Dashboard – Snowflake Development Environment

This repository contains all development assets for the end-to-end data pipeline that powers the **UTP 2026 Dashboard**, including Snowflake SQL scripts, Python ingestion scripts, and environment setup files.

The goal is to maintain a reproducible, version-controlled workflow for ingestion (Bronze), transformations (Silver), and final models/views (Gold).

File naming convention:
"<type>_<operation_type>_<name>_<stage>.<extension>"

where:
    - type: 
        dt - Dynamic Table
        t  - Table
        v  - View
        i  - Initialize
        ff - File Format
    - operation_type:
        join
        filter
        init
        aggregate
        select
        load
        insert
    - name: The name you want to give to the file.
    - stage:
        bronze
        silver
        gold
        ref - reference
    - extension:
        sql    - sql
        python - py

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

pip install snowflake-connector-python

Optional (to reduce SSO pop-ups):
pip install "snowflake-connector-python[secure-local-storage]"

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

## Weekly File Ingestion (Carryovers)

### Purpose
Load weekly carryover CSV (e.g., 2026 Carryover - TTI-v44.csv) into:

SHARVIL_UTP_2026_DASHBOARD.BRONZE.CARRYOVERS

This replaces manual Snowsight uploads.

### Files Involved (bronze/)
- file_loader_framework_bronze.py (framework)
- ingest_carryovers_file_bronze.py (carryover ingestion)

--------------------------------------------------------------------------------

## Pre‑requisites in Snowflake
Run once:
- Create target table
- Create file format

--------------------------------------------------------------------------------

## How the Bronze Loader Works

The loader steps:
1. Connect to Snowflake via externalbrowser
2. USE ROLE / WAREHOUSE / DATABASE / SCHEMA
3. TRUNCATE TABLE (optional)
4. PUT file into @%TABLE_NAME
5. COPY INTO TABLE with the file format
6. Log row count

This logic lives in file_loader_framework_bronze.py.

--------------------------------------------------------------------------------

## How to Run the Weekly Carryover Ingestion

1. Save latest carryover file to:

C:\Users\<you>\Downloads\carryover_files\

2. Update local_file path inside ingest_carryovers_file_bronze.py:

loader.load_file(
    local_file=r"C:\Users\<you>\Downloads\carryover_files\2026 Carryover - TTI-v44.csv",
    table_name="CARRYOVERS",
    file_format="BRONZE.CARRYOVERS_CSV_FF",
    truncate_before_load=True,
    on_error="ABORT_STATEMENT",
)

3. Run:

cd snowflake-dev/bronze
python ingest_carryovers_file_bronze.py

4. Script behavior:
- Browser opens for SSO
- Truncate BRONZE.CARRYOVERS
- PUT file to @%CARRYOVERS
- COPY INTO BRONZE.CARRYOVERS
- Log “BRONZE.CARRYOVERS now has X rows”

5. Optional: Verify in Snowflake.

--------------------------------------------------------------------------------

## Running Transformation Pipeline (Bronze → Silver)

snowsql -f run_pipeline.sql

Or run !source scripts manually.

--------------------------------------------------------------------------------

## Bronze Loader Framework – Details

Located at:
bronze/file_loader_framework_bronze.py

### Components

SnowflakeConfig dataclass:
- account, user, warehouse, database, schema, role, authenticator

BronzeLoader class:
- _connect()
- _set_context()
- load_file():
  - TRUNCATE (optional)
  - PUT file
  - COPY INTO table
  - Log row count

--------------------------------------------------------------------------------

## Reusing Framework for Another Bronze Table

Create a new Python script and call:

loader.load_file(
    local_file="path/to/new.csv",
    table_name="NEW_BRONZE_TABLE",
    file_format="BRONZE.NEW_FILE_FORMAT",
)

No need to rewrite framework logic.

--------------------------------------------------------------------------------

## Maintainer

Sharvil Arjunwadkar
Data Engineer / Data Scientist
Texas A&M Transportation Institute
