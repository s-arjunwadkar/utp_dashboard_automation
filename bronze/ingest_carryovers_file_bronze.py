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

    # Resolve init SQL relative to this script's directory
    base_dir = Path(__file__).parent
    init_sql = base_dir / "t_init_targets_carryovers_bronze.sql"

    # Define file path
    file_path = Path(r"C:\Users\S-Arjunwadkar\Downloads\UTP_Dashboard_Project\input_files\2026_Carryover_TTI_v44.csv")
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # ---------------------------------------------
    # 3. Call load_file for this specific use case
    # ---------------------------------------------
    loader.load_file(
        local_file=str(file_path),
        table_name="CARRYOVERS",                  # no schema here
        file_format="BRONZE.CARRYOVERS_CSV_FF",       # fully-qualified file format
        truncate_before_load=True,                    # full refresh weekly
        on_error="ABORT_STATEMENT",
        init_sql_path=str(init_sql),                  # ensure table exists
    )


if __name__ == "__main__":
    main()
