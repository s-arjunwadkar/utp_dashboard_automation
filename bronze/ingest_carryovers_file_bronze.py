# import logging
# import snowflake.connector
# import sys

# # -----------------------------------------------------
# # LOGGING SETUP
# # -----------------------------------------------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[
#         logging.StreamHandler(sys.stdout),
#         # Optional: enable logging to file
#         # logging.FileHandler("carryovers_loader.log"),
#     ]
# )

# logger = logging.getLogger(__name__)

# # -----------------------------------------------------
# # CONFIGURATION
# # -----------------------------------------------------
# local_file_path = r"C:\Users\S-Arjunwadkar\Downloads\carryover_files\2026 Carryover - TTI-v44.csv"
# table_name = "CARRYOVERS"
# file_format = "BRONZE.CARRYOVERS_CSV_FF"

# # -----------------------------------------------------
# # CONNECT TO SNOWFLAKE (EXTERNALBROWSER)
# # -----------------------------------------------------
# try:
#     logger.info("Connecting to Snowflake using externalbrowser authentication...")
#     conn = snowflake.connector.connect(
#         account="TAM-TI",
#         user="S-ARJUNWADKAR@TTI.TAMU.EDU",
#         warehouse="SHARVIL_UTP_DASHBOARD",
#         database="SHARVIL_UTP_2026_DASHBOARD",
#         schema="BRONZE",
#         role="SYSADMIN",
#         authenticator="externalbrowser",
#     )
# except Exception as e:
#     logger.error(f"Connection failed: {e}")
#     sys.exit(1)

# cur = conn.cursor()

# try:
#     # -----------------------------------------------------
#     # SET CONTEXT
#     # -----------------------------------------------------
#     logger.info("Setting Snowflake context (role, warehouse, database, schema)...")
#     cur.execute("USE ROLE SYSADMIN;")
#     cur.execute("USE WAREHOUSE SHARVIL_UTP_DASHBOARD;")
#     cur.execute("USE DATABASE SHARVIL_UTP_2026_DASHBOARD;")
#     cur.execute("USE SCHEMA BRONZE;")

#     # -----------------------------------------------------
#     # TRUNCATE TARGET TABLE
#     # -----------------------------------------------------
#     logger.info(f"Truncating table BRONZE.{table_name}...")
#     cur.execute(f"TRUNCATE TABLE IF EXISTS BRONZE.{table_name};")

#     # -----------------------------------------------------
#     # PUT LOCAL FILE INTO TABLE STAGE
#     # -----------------------------------------------------
#     logger.info(f"Uploading local file to Snowflake stage @%{table_name} ...")
#     cur.execute(fr"""
#         PUT file://{local_file_path}
#             @%{table_name}
#             AUTO_COMPRESS = TRUE;
#     """)

#     # -----------------------------------------------------
#     # COPY INTO TARGET TABLE
#     # -----------------------------------------------------
#     logger.info("Copying staged data into target table...")
#     cur.execute(fr"""
#         COPY INTO BRONZE.{table_name}
#         FROM @%{table_name}
#         FILE_FORMAT = (FORMAT_NAME = {file_format})
#         ON_ERROR = 'ABORT_STATEMENT';
#     """)

#     logger.info("✔ SUCCESS — Carryovers file loaded into BRONZE.CARRYOVERS_V43")

# except Exception as e:
#     logger.error(f"❌ ERROR during data load: {e}")
#     sys.exit(1)

# finally:
#     cur.close()
#     conn.close()
#     logger.info("Snowflake session closed.")

from file_loader_framework_bronze import BronzeLoader, SnowflakeConfig


def main():
    # ---------------------------------------------
    # 1. Configure Snowflake connection
    # ---------------------------------------------
    config = SnowflakeConfig(
        account="TAM-TI",
        user="S-ARJUNWADKAR@TTI.TAMU.EDU",
        warehouse="SHARVIL_UTP_DASHBOARD",
        database="SHARVIL_UTP_2026_DASHBOARD",
        schema="BRONZE",
        role="SYSADMIN",
        authenticator="externalbrowser",
    )

    # ---------------------------------------------
    # 2. Create loader instance
    # ---------------------------------------------
    loader = BronzeLoader(config)

    # ---------------------------------------------
    # 3. Call load_file for this specific use case
    # ---------------------------------------------
    loader.load_file(
        local_file=r"C:\Users\S-Arjunwadkar\Downloads\carryover_files\2026 Carryover - TTI-v44.csv",
        table_name="CARRYOVERS",                  # no schema here
        file_format="BRONZE.CARRYOVERS_CSV_FF",       # fully-qualified file format
        truncate_before_load=True,                    # full refresh weekly
        on_error="ABORT_STATEMENT",
    )


if __name__ == "__main__":
    main()
