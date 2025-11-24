import logging
import sys
from dataclasses import dataclass
from typing import Optional

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

    def load_file(
        self,
        local_file: str,
        table_name: str,
        file_format: str,
        truncate_before_load: bool = True,
        on_error: str = "ABORT_STATEMENT",
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
        conn = self._connect()
        cur = conn.cursor()

        fq_table = f"{self.config.schema}.{table_name}"  # e.g. BRONZE.CARRYOVERS_V43

        try:
            # 1) Context
            self._set_context(cur)

            # 2) Truncate if requested
            if truncate_before_load:
                logger.info(f"Truncating target table {fq_table} ...")
                cur.execute(f"TRUNCATE TABLE {fq_table};")

            # 3) PUT local file -> table stage (@%TABLE)
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


            # 4) COPY INTO table
            logger.info(f"Copying data from @%{table_name} into {fq_table} ...")
            copy_sql = rf"""
                COPY INTO {fq_table}
                FROM @%{table_name}
                FILE_FORMAT = (FORMAT_NAME = {file_format})
                ON_ERROR = '{on_error}';
            """
            logger.debug(f"COPY SQL:\n{copy_sql}")
            cur.execute(copy_sql)

            # 5) Optional: log row count after load
            cur.execute(f"SELECT COUNT(*) FROM {fq_table};")
            row_count = cur.fetchone()[0]
            logger.info(f"✔ Load complete. {fq_table} now has {row_count} rows.")

        except Exception as e:
            logger.error(f"❌ Error during load_file for {fq_table}: {e}")
            raise
        finally:
            logger.info("Closing Snowflake cursor and connection.")
            cur.close()
            conn.close()
