USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE VIEW BRONZE.TARGETS_CARRYOVER_FILE
COMMENT = 'This is the carryovers file that we get each week. Latest version: V43'
AS
SELECT
    category,
    district_mpo_division,
    carryovers,
    fy_2026,
    fy_2027,
    fy_2028,
    fy_2029,
    fy_2030,
    fy_2031,
    fy_2032,
    fy_2033,
    fy_2034,
    fy_2035
FROM BRONZE.CARRYOVERS_V43
WHERE district_mpo_division <> 'TOTAL';