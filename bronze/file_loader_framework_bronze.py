import logging
import sys
from dataclasses import dataclass
from typing import Optional
import os
import re

import snowflake.connector


# -----------------------------------------------------
# LOGGING SETUP (framework-wide)
# -----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


# -----------------------------------------------------
# CONFIG OBJECT (for clarity + reuse)
# -----------------------------------------------------
@dataclass
class SnowflakeConfig:
    account: str
    user: str
    warehouse: str
    database: str
    schema: str
    role: str = "SYSADMIN"
    authenticator: str = "externalbrowser"  # using SSO / browser login


# -----------------------------------------------------
# BRONZE LOADER FRAMEWORK
# -----------------------------------------------------
class BronzeLoader:
    """
    Small reusable helper for loading local files into Bronze tables in Snowflake.

    Default pattern (backward compatible):
      1. Connect to Snowflake (externalbrowser)
      2. Set context (role, warehouse, db, schema)
      3. REMOVE old staged copies from table stage (@%TABLE)
      4. PUT local file -> table stage (@%TABLE)
      5. BEGIN; TRUNCATE TABLE; COPY INTO; COMMIT (rollback-safe)

    Extensions (optional):
      - stage_name="BRONZE.CHANGE_ORDERS_STAGE" to use a named stage (@BRONZE.CHANGE_ORDERS_STAGE)
      - do_copy=False to stop after PUT (stage-only ingest for dynamic-schema files)
    """

    def __init__(self, config: SnowflakeConfig):
        self.config = config

    def _connect(self):
        """
        Create and return a Snowflake connection using externalbrowser.
        """
        logger.info("Connecting to Snowflake using externalbrowser authenticator...")
        try:
            conn = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                warehouse=self.config.warehouse,
                database=self.config.database,
                schema=self.config.schema,
                role=self.config.role,
                authenticator=self.config.authenticator,
            )
            logger.info("Snowflake connection established.")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def _set_context(self, cur):
        """
        Ensure the correct role / warehouse / db / schema are active.
        """
        logger.info("Setting Snowflake context (role, warehouse, database, schema)...")
        cur.execute(f"USE ROLE {self.config.role};")
        cur.execute(f"USE WAREHOUSE {self.config.warehouse};")
        cur.execute(f"USE DATABASE {self.config.database};")
        cur.execute(f"USE SCHEMA {self.config.schema};")

    def _ensure_table_exists(self, cur, table_name: str, init_sql_path: Optional[str] = None):
        """
        Check if the target table exists; if not, optionally run an init SQL script.
        """
        db = self.config.database
        schema = self.config.schema
        table_upper = table_name.upper()

        logger.info(f"Checking if table {db}.{schema}.{table_upper} exists...")

        check_sql = """
            SELECT 1
            FROM {db}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = %s
              AND TABLE_SCHEMA  = %s
              AND TABLE_NAME    = %s
            LIMIT 1;
        """.format(db=db)

        cur.execute(check_sql, (db, schema, table_upper))
        exists = cur.fetchone() is not None

        if exists:
            logger.info(f"Table {db}.{schema}.{table_upper} already exists. Skipping init script.")
            return

        logger.warning(f"Table {db}.{schema}.{table_upper} does NOT exist.")

        if not init_sql_path:
            raise RuntimeError(
                f"Table {db}.{schema}.{table_upper} is missing and no init_sql_path was provided."
            )

        logger.info(f"Running init SQL script to create it: {init_sql_path}")

        with open(init_sql_path, "r", encoding="utf-8") as f:
            sql_script = f.read()

        statements = [s.strip() for s in sql_script.split(";") if s.strip()]
        for stmt in statements:
            logger.debug(f"Executing init statement:\n{stmt}")
            cur.execute(stmt)

        logger.info(f"Init script {init_sql_path} executed. Table {db}.{schema}.{table_upper} should now exist.")

    def _remove_matching_stage_files(self, cur, stage_target: str, local_file: str) -> str:
        """
        Remove staged files in the given stage_target that match the local filename (incl .gz).

        stage_target examples:
          - "@%TABLE_NAME" (table stage)
          - "@BRONZE.CHANGE_ORDERS_STAGE" (named stage)
        """
        base_name = os.path.basename(local_file)   # e.g., "change_orders.csv"
        escaped = re.escape(base_name)

        # Safer than ^...$ in case stage paths include prefixes later.
        # Still matches current observed behavior: filename.csv.gz.
        pattern = rf".*{escaped}(\.gz)?$"

        sql = f"REMOVE {stage_target} PATTERN='{pattern}';"
        logger.info(f"Removing old staged files in {stage_target} matching: {base_name}")
        logger.debug(f"REMOVE SQL:\n{sql}")
        cur.execute(sql)

        return pattern

    def _build_copy_with_ingested_at(
        self,
        cur,
        table_name: str,
        file_format: str,
        on_error: str,
        stage_target: str,
        pattern: Optional[str] = None,
    ) -> str:
        """
        Build a COPY INTO statement that:
          - Reads from stage_target using the provided FILE_FORMAT
          - Maps all file columns ($1..$N-1) to table columns
          - Appends CURRENT_TIMESTAMP() as INGESTED_AT (last column)

        Assumptions:
          - Target table's last column is INGESTED_AT
          - File has exactly (number_of_table_columns - 1) columns
        """
        db = self.config.database
        schema = self.config.schema
        table_upper = table_name.upper()
        fq_table = f"{schema}.{table_name}"

        cur.execute(
            f"""
            SELECT COLUMN_NAME
            FROM {db}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_CATALOG = %s
              AND TABLE_SCHEMA  = %s
              AND TABLE_NAME    = %s
            ORDER BY ORDINAL_POSITION
            """,
            (db, schema, table_upper),
        )

        cols = [row[0] for row in cur.fetchall()]
        if not cols:
            raise RuntimeError(f"No columns found for table {db}.{schema}.{table_upper}")

        last_col = cols[-1].upper()
        if last_col != "INGESTED_AT":
            raise RuntimeError(
                f"Timestamp-append pattern requires last column to be INGESTED_AT, "
                f"but last column is {last_col} for {db}.{schema}.{table_upper}."
            )

        num_table_cols = len(cols)
        num_file_cols = num_table_cols - 1
        if num_file_cols < 1:
            raise RuntimeError(
                f"Table {db}.{schema}.{table_upper} must have at least 2 columns "
                "(data columns + INGESTED_AT)."
            )

        file_exprs = [f"${i}" for i in range(1, num_file_cols + 1)]
        select_exprs = file_exprs + ["CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"]
        select_clause = ",\n    ".join(select_exprs)

        pattern_clause = f",\n                PATTERN => '{pattern}'" if pattern else ""

        copy_sql = f"""
            COPY INTO {fq_table}
            FROM (
                SELECT
                    {select_clause}
                FROM {stage_target} (
                    FILE_FORMAT => {file_format}{pattern_clause}
                )
            )
            ON_ERROR = '{on_error}';
        """
        return copy_sql

    def load_file(
        self,
        local_file: str,
        table_name: str,
        file_format: str,
        truncate_before_load: bool = True,
        on_error: str = "ABORT_STATEMENT",
        init_sql_path: Optional[str] = None,
        stage_name: Optional[str] = None,   # use named stage instead of table stage
        do_copy: bool = True,               # if False, stop after PUT (stage-only)
    ):
        """
        Load a local file into Snowflake.

        Backward-compatible defaults:
          - stage_name=None -> uses table stage @%{table_name}
          - do_copy=True -> TRUNCATE + COPY into table

        For dynamic-schema sources (Change Orders):
          - stage_name="BRONZE.CHANGE_ORDERS_STAGE"
          - do_copy=False   (PUT only; Snowflake-side procedure builds tables)
        """
        """
        Stage cleanup and PUT are non-transactional.
        TRUNCATE + COPY are transactional and rollback-safe.
        """
        conn = self._connect()
        cur = conn.cursor()

        fq_table = f"{self.config.schema}.{table_name}"
        in_txn = False

        # Stage target:
        # - named stage: @BRONZE.CHANGE_ORDERS_STAGE
        # - table stage: @%TABLE
        stage_target = f"@{stage_name}" if stage_name else f"@%{table_name}"

        try:
            # 1) Context
            self._set_context(cur)

            # 1.5) Ensure table exists (if init script provided) ONLY if plan to copy into it
            if do_copy and init_sql_path is not None:
                self._ensure_table_exists(cur, table_name, init_sql_path)

            # 2) Stage cleanup + PUT
            pattern = self._remove_matching_stage_files(cur, stage_target, local_file)

            logger.info(f"Uploading local file to stage {stage_target} ...")
            normalized_path = local_file.replace("\\", "/")

            put_sql = rf"""
                PUT 'file://{normalized_path}'
                    {stage_target}
                    AUTO_COMPRESS = TRUE
                    OVERWRITE = TRUE;
            """
            logger.debug(f"PUT SQL:\n{put_sql}")
            cur.execute(put_sql)

            # If this ingest is stage-only (dynamic schema), stop here.
            if not do_copy:
                logger.info("do_copy=False -> finished staging (no TRUNCATE/COPY).")
                return

            # ✅ 2.5) Start transaction for table changes only
            cur.execute("BEGIN;")
            in_txn = True

            # 3) Truncate if requested
            if truncate_before_load:
                logger.info(f"Truncating target table {fq_table} ...")
                cur.execute(f"TRUNCATE TABLE {fq_table};")

            # 4) COPY INTO table with auto-ingested timestamp
            logger.info(f"Copying data from {stage_target} into {fq_table} with INGESTED_AT...")
            copy_sql = self._build_copy_with_ingested_at(
                cur=cur,
                table_name=table_name,
                file_format=file_format,
                on_error=on_error,
                stage_target=stage_target,
                pattern=pattern,
            )
            logger.debug(f"COPY SQL:\n{copy_sql}")
            cur.execute(copy_sql)

            # ✅ COMMIT so changes are durable
            cur.execute("COMMIT;")
            in_txn = False

            # 5) Optional: log row count after load
            cur.execute(f"SELECT COUNT(*) FROM {fq_table};")
            row_count = cur.fetchone()[0]
            logger.info(f"✔ Load complete. {fq_table} now has {row_count} rows.")

        except Exception as e:
            # ✅ rollback ONLY if started a transaction
            if in_txn:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass

            logger.error(f"❌ Error during load_file for {fq_table}: {e}")
            raise

        finally:
            logger.info("Closing Snowflake cursor and connection.")
            cur.close()
            conn.close()