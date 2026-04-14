USE DATABASE UTP_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;
USE ROLE SYSADMIN;
-- USE SCHEMA GOLD;

CREATE OR REPLACE VIEW GOLD.V_CSJ_LEVEL_OUTPUT_1_GOLD
COMMENT = 'This view provides the output at a csj level to be connected to Tableau for the dashboard. It includes authorized amounts from project details with LET and Cost overruns adjustments.' 
AS
SELECT
    district_mpo_division,
    district,
    csj,
    county,
    highway,
    limits_from,
    limits_to,
    project_description,
    category, 
    fy,
    let_type_description,
    let_sch_fy,
    authorized_amount,
    orig_authorized_amount,
    db_or_dbb
FROM SILVER.V_PD_PRE_OUTPUT
ORDER BY district_mpo_division, district, csj, category, fy;

CREATE OR REPLACE SECURE VIEW GOLD.SECURE_V_CSJ_LEVEL_OUTPUT_1_GOLD
COMMENT = 'This secure view provides the output at a csj level to be connected to Tableau for the dashboard. It includes authorized amounts from project details with LET and Cost overruns adjustments.' 
AS
SELECT
    district_mpo_division,
    district,
    csj,
    county,
    highway,
    limits_from,
    limits_to,
    project_description,
    category, 
    fy,
    let_type_description,
    let_sch_fy,
    authorized_amount,
    orig_authorized_amount,
    db_or_dbb
FROM SILVER.V_PD_PRE_OUTPUT
ORDER BY district_mpo_division, district, csj, category, fy;
