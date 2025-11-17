USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE VIEW SILVER.TOTAL_TARGETS
COMMENT = 'Unpivot the targets and carryover file and calculate total targets by each category, district_mpo_division and FY.'
AS
WITH unpivot_targets AS (
    SELECT *
    FROM BRONZE.TARGETS_CARRYOVER_FILE
    UNPIVOT INCLUDE NULLS (targets FOR FY IN (fy_2026, fy_2027, fy_2028, fy_2029, fy_2030, fy_2031, fy_2032, fy_2033, fy_2034, fy_2035))
    ORDER BY CATEGORY
),

formatted_targets AS (
SELECT
    category,
    CASE
        WHEN category = '6' THEN 'Bridge Division'
        WHEN category = '8' THEN 'Traffic Division'
        WHEN category = '10' THEN 'Supplemental Transportation Projects'
        ELSE district_mpo_division
    END AS district_mpo_division,
    TRIM(SPLIT_PART(FY, '_', 2)) AS fy,
    carryovers,
    targets
FROM unpivot_targets
),

cat_8 AS (
    SELECT
        category,
        district_mpo_division,
        fy,
        SUM(carryovers) AS carryovers,
        SUM(targets) AS targets
    FROM formatted_targets
    WHERE category = '8'
    GROUP BY category, district_mpo_division, fy
),

targets_carryovers AS (
    SELECT * FROM formatted_targets
    WHERE category != '8'
    UNION ALL
    SELECT * FROM cat_8
)

SELECT
    category,
    district_mpo_division,
    fy,
    carryovers,
    SUM(targets) AS total_targets
FROM targets_carryovers
GROUP BY category, district_mpo_division, fy, carryovers
ORDER BY category, district_mpo_division, fy
;