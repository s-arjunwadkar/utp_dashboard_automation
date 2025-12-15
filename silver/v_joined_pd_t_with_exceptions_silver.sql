USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE VIEW SILVER.TOTAL_WITH_EXCPN_VIEW
COMMENT = 'This tables appends the exceptions in target file to the joined project details and targets table.'
AS
WITH exceptions AS ( 
SELECT
    category::STRING AS category,
    district_mpo_division::STRING AS district_mpo_division,
    fy::STRING AS fy,
    0 AS total_authorized_amount,
    total_targets
FROM SILVER.EXCEPTION_TARGETS
)

SELECT * FROM SILVER.JOINED_PD_TARGET_VIEW
UNION ALL
SELECT * FROM exceptions
ORDER BY category, district_mpo_division, fy
;

-- SELECT * FROM SILVER.TOTAL_WITH_EXCPN_VIEW;