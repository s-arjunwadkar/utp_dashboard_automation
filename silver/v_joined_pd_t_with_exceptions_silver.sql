USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE VIEW SILVER.TOTAL_WITH_EXCPN_VIEW
COMMENT = 'This tables appends the exceptions in target file to the joined project details and targets table.'
AS
WITH exceptions AS ( 
SELECT
    category,
    district_mpo_division,
    fy,
    0 AS total_authorized_amount,
    total_targets,
    carryovers
FROM SILVER.EXCEPTION_TARGETS
),

normal_exceptions_union AS (
    SELECT * FROM SILVER.JOINED_PD_TARGET_VIEW
    UNION ALL
    SELECT * FROM exceptions
    ORDER BY category, district_mpo_division, fy
)

SELECT
    category,
    district_mpo_division,
    CASE
        WHEN fy = 2026 THEN '2026 + Carryovers'
        ELSE CAST(fy AS STRING)
    END AS CAST(fy AS STRING) AS fy,
    total_authorized_amount + carryovers AS total_authorized_amount,
    total_targets,
    carryovers
FROM normal_exceptions_union
ORDER BY category, district_mpo_division, fy
;
-- SELECT * FROM SILVER.TOTAL_WITH_EXCPN_VIEW;