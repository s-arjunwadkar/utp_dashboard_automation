USE ROLE SYSADMIN;
USE DATABASE UTP_DASHBOARD;
USE WAREHOUSE UTP_DASHBOARD_WH;
USE SCHEMA GOLD;

CREATE OR REPLACE VIEW GOLD.V_DELTA_REPORT_OUTPUT_GOLD
COMMENT = 'This view provides the delta report output at a csj, district, and MPO level to be connected to Tableau for the dashboard. It includes authorized amounts from project details with LET and Cost overruns adjustments.'
AS
SELECT
    csj,
    district_mpo_division,
    category,
    fy,
    authorized_amount,
    authorized_amount_yesterday,
    authorized_amount_difference,
    change_status,
    compared_at
FROM GOLD.T_DELTA_REPORT_OUTPUT_GOLD;
CREATE OR REPLACE SECURE VIEW GOLD.V_DELTA_REPORT_OUTPUT_GOLD
COMMENT = 'This secure view provides the delta report output at a csj, district, and MPO level to be connected to Tableau for the dashboard. It includes authorized amounts from project details with LET and Cost overruns adjustments.'
AS
SELECT
    csj,
    district_mpo_division,
    category,
    fy,
    authorized_amount,
    authorized_amount_yesterday,
    authorized_amount_difference,
    change_status,
    compared_at
FROM GOLD.T_DELTA_REPORT_OUTPUT_GOLD;
 