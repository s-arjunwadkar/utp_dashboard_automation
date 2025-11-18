USE ROLE SYSADMIN;
USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

-- Bronze Layer
!source bronze/v_load_targets_carryovers_bronze.sql;
!source bronze/dt_extract_project_details_bronze.sql;

-- Silver Layer
!source silver/dt_filter_project_details_silver.sql;
!source silver/dt_join_project_details_new_cat_silver.sql;
!source silver/dt_join_project_details_mpo_silver.sql;

!source silver/v_agg_total_targets_silver.sql;
!source silver/v_join_targets_scope_silver.sql;
!source silver/v_select_targets_normal_exceptions_silver.sql;

!source silver/v_agg_project_details_total_programmed_silver.sql;

!source silver/v_join_project_details_targets_silver.sql;
!source silver/v_joined_pd_t_with_exceptions_silver.sql;

SELECT 'Pipeline executed successfully at ' || CURRENT_TIMESTAMP();
