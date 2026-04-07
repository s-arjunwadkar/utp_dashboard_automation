USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;
USE ROLE SYSADMIN;
-- USE SCHEMA GOLD;

CREATE OR REPLACE VIEW GOLD.V_CAT_DIST_MPO_LEVEL_OUTPUT_2_GOLD
COMMENT = 'This view provides the output at a category, district, and MPO level to be connected to Tableau for the dashboard. It includes authorized amounts from project details with LET and Cost overruns adjustments as well as targets, carryovers and change orders amounts.' 
AS
SELECT
    category,
    district_mpo_division,
    fy,
    total_authorized_amount,
    total_targets,
    carryovers,
    targets_carryovers_combined,
    change_orders_amount
FROM SILVER.V_JOIN_PD_ALL_CHANGE_ORDERS_SILVER
ORDER BY category, district_mpo_division, fy;