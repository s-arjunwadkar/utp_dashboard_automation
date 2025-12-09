USE WAREHOUSE IF EXISTS SHARVIL_UTP_DASHBOARD;
USE DATABASE IF EXISTS SHARVIL_UTP_2026_DASHBOARD;
USE ROLE SYSADMIN;

-- Initialize MPO Table
!source ref\t_init_mpo_reference_ref.sql;

-- Initialize New Category Table
!source ref\t_init_category_map_rf.sql;
!source ref\i_inserts_category_map_rf.sql;
!source ref\v_init_current_cat_map_rf.sql;