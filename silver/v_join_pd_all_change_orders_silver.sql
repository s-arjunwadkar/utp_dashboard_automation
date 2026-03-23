USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE SCHEMA SILVER;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE VIEW SHARVIL_UTP_2026_DASHBOARD.SILVER.V_JOIN_PD_ALL_CHANGE_ORDERS_SILVER
COMMENT = 'Join the change orders with the project details data.' 
AS
WITH final_w_o_fy26 AS (
    SELECT *,
    0 AS CHANGE_ORDERS_AMOUNT
    FROM SILVER.TOTAL_WITH_EXCPN_VIEW
    WHERE NOT FY ILIKE '2026%'
),

with_change_orders AS (
    SELECT m.*, c.CHANGE_ORDERS_AMOUNT
    FROM SILVER.TOTAL_WITH_EXCPN_VIEW AS m
    LEFT JOIN SILVER.V_CHANGE_ORDERS_SILVER AS c
    ON m.category = c.category
    AND LOWER(m.district_mpo_division) = LOWER(c.district_mpo_division)
    WHERE m.fy ILIKE '2026%'
),

agg_with_co AS (
SELECT
    category,
    district_mpo_division,
    fy,
    total_authorized_amount,
    total_targets,
    carryovers,
    SUM(COALESCE(CHANGE_ORDERS_AMOUNT, 0)) AS change_orders_amount
FROM with_change_orders
GROUP BY category, district_mpo_division, fy, total_authorized_amount, total_targets, carryovers
)

SELECT * FROM final_w_o_fy26
UNION ALL
SELECT * FROM agg_with_co
ORDER BY category, district_mpo_division, fy
;