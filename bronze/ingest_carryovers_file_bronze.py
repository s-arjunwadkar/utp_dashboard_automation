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
