from file_loader_framework_bronze import BronzeLoader, SnowflakeConfig
from pathlib import Path


def main():
    # ---------------------------------------------
    # 1. Configure Snowflake connection
    # ---------------------------------------------
    config = SnowflakeConfig(
        account="TAM-PPM",
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
    # 3. File path (REQUIRED)
    # ---------------------------------------------
    file_path = Path(
        r"C:\Users\S-Arjunwadkar\Downloads\UTP_Dashboard_Project\input_files\change_orders.csv"
    )
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # ---------------------------------------------
    # 4. Stage-only ingest (NO COPY INTO TABLE)
    # ---------------------------------------------
    loader.load_file(
        local_file=str(file_path),

        # table_name is ignored when stage_name is provided,
        # but still required by the framework signature
        table_name="CHANGE_ORDERS",

        file_format="BRONZE.CHANGE_ORDERS_CSV_FF",

        truncate_before_load=False,   # irrelevant here
        on_error="ABORT_STATEMENT",
        init_sql_path=None,           # no table init

        stage_name="BRONZE.CHANGE_ORDERS_STAGE",
        do_copy=False,                # KEY FLAG
    )


if __name__ == "__main__":
    main()