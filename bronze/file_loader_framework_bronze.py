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

    Pattern:
      1. Connect to Snowflake (externalbrowser)
      2. Set context (role, warehouse, db, schema)
      3. Optional TRUNCATE TABLE
      4. PUT local file -> table stage (@%TABLE)
      5. COPY INTO TABLE FROM @%TABLE FILE_FORMAT = (...)
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
        Check if the target table exists; if not, optionally run an init SQL script
        (e.g. a CREATE TABLE statement).

        Parameters
        ----------
        cur : cursor
            Active Snowflake cursor.
        table_name : str
            Table name WITHOUT schema (schema comes from config).
        init_sql_path : str, optional
            Path to a .sql file that creates the table (and related objects).
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

        # Very simple splitter: assumes your .sql file has 1 or a few ';'-terminated statements
        with open(init_sql_path, "r", encoding="utf-8") as f:
            sql_script = f.read()

        statements = [s.strip() for s in sql_script.split(";") if s.strip()]
        for stmt in statements:
            logger.debug(f"Executing init statement:\n{stmt}")
            cur.execute(stmt)

        logger.info(f"Init script {init_sql_path} executed. Table {db}.{schema}.{table_upper} should now exist.")

    def _remove_matching_stage_files(self, cur, table_name: str, local_file: str):
        """
        Remove staged files in @%table_name that match the local filename (incl .gz).
        This prevents old files from being reloaded.
        """
        base_name = os.path.basename(local_file)           # e.g., "costoverruns.csv"
        escaped = re.escape(base_name)                     # escape regex special chars
        pattern = rf"^{escaped}(\.gz)?$"                         # matches costoverruns.csv and costoverruns.csv.gz

        sql = f"REMOVE @%{table_name} PATTERN='{pattern}';"
        logger.info(f"Removing old staged files in @%{table_name} matching: {base_name}")
        logger.debug(f"REMOVE SQL:\n{sql}")
        cur.execute(sql)

        return pattern  # return pattern so COPY can use it too

    def _build_copy_with_ingested_at(self, cur, table_name: str, file_format: str, on_error: str, pattern: Optional[str] = None,) -> str:
        """
        Build a COPY INTO statement that:
          - Reads from @%TABLE using the provided FILE_FORMAT
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

        # Fetch table columns in order
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
            raise RuntimeError(
                f"No columns found for table {db}.{schema}.{table_upper}"
            )

        # Validate last column
        last_col = cols[-1].upper()
        if last_col != "INGESTED_AT":
            raise RuntimeError(
            f"Timestamp-append pattern requires last column to be INGESTED_AT, "
            f"but last column is {last_col} for {db}.{schema}.{table_upper}."
        )

        num_table_cols = len(cols)
        num_file_cols = num_table_cols - 1 # all but INGESTED_AT

        if num_file_cols < 1:
            raise RuntimeError(
                f"Table {db}.{schema}.{table_upper} must have at least 2 columns "
                "(data columns + INGESTED_AT)."
            )

        # Build $1, $2, ..., $N-1 expressions
        file_exprs = [f"${i}" for i in range(1, num_file_cols + 1)]

        # Add CURRENT_TIMESTAMP() for the last column
        select_exprs = file_exprs + ["CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"]
        select_clause = ",\n    ".join(select_exprs)

        pattern_clause = f",\n                PATTERN => '{pattern}'" if pattern else ""

        copy_sql = f"""
            COPY INTO {fq_table}
            FROM (
                SELECT
                    {select_clause}
                FROM @%{table_name} (
                FILE_FORMAT => {file_format}{pattern_clause}
                )
            )
            ON_ERROR = '{on_error}';"""
        
        return copy_sql



    def load_file(
        self,
        local_file: str,
        table_name: str,
        file_format: str,
        truncate_before_load: bool = True,
        on_error: str = "ABORT_STATEMENT",
        init_sql_path: Optional[str] = None,
    ):
        """
        Load a local CSV (or other) file into a Bronze table.

        Parameters
        ----------
        local_file : str
            Full path to the local file on your machine.
        table_name : str
            Target table name (without schema). The schema comes from config.
        file_format : str
            Fully-qualified file format name, e.g. 'BRONZE.CARRYOVERS_CSV_FF'.
        truncate_before_load : bool
            If True, TRUNCATE TABLE before loading (weekly full refresh pattern).
        on_error : str
            COPY INTO ON_ERROR behavior (e.g., 'ABORT_STATEMENT', 'CONTINUE').
        """
        """
        Stage cleanup and PUT are non-transactional.
        TRUNCATE + COPY are transactional and rollback-safe.
        """
        conn = self._connect()
        cur = conn.cursor()

        fq_table = f"{self.config.schema}.{table_name}"  # e.g. BRONZE.CARRYOVERS_V43
        in_txn = False


        try:
            # 1) Context
            self._set_context(cur)

            # 1.5) Ensure table exists (if init script provided)
            if init_sql_path is not None:
                self._ensure_table_exists(cur, table_name, init_sql_path)

            # 2) PUT local file -> table stage (@%TABLE)
            # Remove any old staged copies of the same file first
            pattern = self._remove_matching_stage_files(cur, table_name, local_file)

            logger.info(f"Uploading local file to stage @%{table_name} ...")

            # Normalize path (optional but nice: convert backslashes to forward slashes)
            normalized_path = local_file.replace("\\", "/")

            put_sql = rf"""
                PUT 'file://{normalized_path}'
                    @%{table_name}
                    AUTO_COMPRESS = TRUE;
            """
            logger.debug(f"PUT SQL:\n{put_sql}")
            cur.execute(put_sql)

            # ✅ 2.5) Start transaction for table changes only
            cur.execute("BEGIN;")
            in_txn = True

            # 3) Truncate if requested
            if truncate_before_load:
                logger.info(f"Truncating target table {fq_table} ...")
                cur.execute(f"TRUNCATE TABLE {fq_table};")

            # 4) COPY INTO table with auto-ingested timestamp
            logger.info(f"Copying data from @%{table_name} into {fq_table} with INGESTED_AT...")
            copy_sql = self._build_copy_with_ingested_at(
                cur=cur,
                table_name=table_name,
                file_format=file_format,
                on_error=on_error,
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
            # ✅ rollback ONLY if we started a transaction
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
