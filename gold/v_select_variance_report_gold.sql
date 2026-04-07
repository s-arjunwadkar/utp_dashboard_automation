USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;
USE ROLE SYSADMIN;
-- USE SCHEMA GOLD;

CREATE OR REPLACE VIEW GOLD.V_VARIANCE_REPORT_GOLD
COMMENT = 'This view provides the output at a CSJ level to be connected to Tableau for the dashboard. It provides information about the errors discovered in the process and information about the error, csj, etc.' 
AS
SELECT *
FROM SILVER.PD_MISSING_MPO_DESC
WHERE category != '3'
ORDER BY category, district_division, estimated_fiscal_year;