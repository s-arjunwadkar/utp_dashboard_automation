USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE SCHEMA SILVER;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE VIEW SHARVIL_UTP_2026_DASHBOARD.SILVER.V_CHANGE_ORDERS_SILVER
COMMENT = 'View to clean and categorize the change orders data from bronze layer. It includes logic to group certain metrics, update district names using project details table and join with MPO reference table to get the correct MPO names and district abbreviations.' 
AS
WITH change_orders_init AS (
SELECT
    DISTRICT,
    CASE
        WHEN METRIC_NAME ILIKE '4_URBAN%' THEN '4_URBAN_CONNECTIVITY_ALL'
        WHEN METRIC_NAME ILIKE '11_BORDER%' THEN '11_RIDER_11($2.1B)'
        ELSE METRIC_NAME
    END AS METRIC_NAME,
    METRIC_VALUE
FROM BRONZE.CHANGE_ORDERS
),

co_2_group AS (
SELECT
    DISTRICT,
    '2_METRO/URBAN' AS METRIC_NAME,
    SUM(METRIC_VALUE) AS METRIC_VALUE
FROM change_orders_init
WHERE METRIC_NAME ILIKE '2%'
GROUP BY DISTRICT
),

co_8_group AS (
SELECT
    DISTRICT,
    '8_TRAFFIC' AS METRIC_NAME,
    SUM(METRIC_VALUE) AS METRIC_VALUE
FROM change_orders_init
WHERE METRIC_NAME ILIKE '8%'
GROUP BY DISTRICT
),

co_10_group AS (
SELECT
    DISTRICT,
    '10_SUPPLEMENTAL' AS METRIC_NAME,
    SUM(METRIC_VALUE) AS METRIC_VALUE
FROM change_orders_init
WHERE METRIC_NAME ILIKE '10%'
GROUP BY DISTRICT
),

co_intermediate AS (
SELECT *
FROM change_orders_init
WHERE NOT METRIC_NAME ILIKE ANY ('2%', '8%', '10%')
UNION ALL
SELECT *
FROM co_2_group
UNION ALL
SELECT *
FROM co_8_group
UNION ALL
SELECT *
FROM co_10_group
),

co_cat AS (
SELECT 
    DISTRICT,
    SPLIT_PART(METRIC_NAME, '_', 1) AS CATEGORY,
    METRIC_NAME,
    METRIC_VALUE
FROM co_intermediate
),

pd_dist_abbr AS (
    SELECT DISTINCT DISTRICT_DIVISION_ABBR, DISTRICT_DIVISION
    FROM SILVER.PROJECT_DETAILS_FILTERED_SILVER
),

co_updated_dist AS (
    SELECT
        co.*,
        pd.DISTRICT_DIVISION_ABBR
    FROM co_cat AS co
    LEFT JOIN pd_dist_abbr AS pd
    ON LOWER(TRIM(co.DISTRICT)) = LOWER(TRIM(pd.DISTRICT_DIVISION))
),
    
co_update_mpo AS (
SELECT
    co.DISTRICT,
    CASE
        WHEN co.DISTRICT_DIVISION_ABBR IS NULL THEN mpo.district_abbr
        ELSE co.DISTRICT_DIVISION_ABBR
    END AS DISTRICT_DIVISION_ABBR,
    CASE
        WHEN co.DISTRICT = mpo.change_orders_mpo AND mpo.change_orders_mpo != '' THEN mpo.mpo_short
        WHEN co.DISTRICT = '[BMT] JHORTS MPO' THEN 'HGAC MPO'
        ELSE co.DISTRICT
    END AS DISTRICT_MPO_NEW,
    co.CATEGORY,
    co.METRIC_NAME,
    co.METRIC_VALUE
FROM co_updated_dist AS co
LEFT JOIN REF.MPO_REFERENCE AS mpo
ON co.DISTRICT = mpo.change_orders_mpo
),

change_orders_final AS (
    SELECT
        CASE
            WHEN DISTRICT = '[BMT] JHORTS MPO' THEN 'HOU/BMT'
            ELSE DISTRICT_DIVISION_ABBR
        END AS DISTRICT_DIVISION_ABBR,
        DISTRICT,
        DISTRICT_MPO_NEW,
        CASE
            WHEN CATEGORY = '4' AND METRIC_NAME ILIKE '4_U%' THEN '4U'
            WHEN CATEGORY = '4' AND METRIC_NAME ILIKE '4_R%' THEN '4R'
            WHEN CATEGORY = '11' AND METRIC_NAME ILIKE '11_DISTRICT_SAF%' THEN '11SF'
            WHEN CATEGORY = '11' AND METRIC_NAME ILIKE '11_PES' THEN '11ES'
            ELSE CATEGORY
        END AS CATEGORY_NEW,
        METRIC_NAME,
        CASE
            WHEN METRIC_NAME ILIKE '11_COCO' THEN 'Cost Overruns/Change Orders'
            WHEN METRIC_NAME ILIKE '11_RI%' THEN 'Rider 11B Program'
            -- WHEN METRIC_NAME ILIKE '11_PES' THEN 'Statewide - Cat 11ES' -- Needs to be updated as it should be district!!
            WHEN METRIC_NAME ILIKE '10_S%' THEN 'Supplemental Transportation Projects' 
            WHEN METRIC_NAME ILIKE '9_TAP_F%' THEN 'Transportation Alternatives Flex Program'
            WHEN METRIC_NAME ILIKE '9_TAP_ST%' THEN 'Transportation Alternatives Program - Non-TMAs'
            WHEN METRIC_NAME ILIKE '8_%' THEN 'Traffic Division'
            WHEN METRIC_NAME ILIKE '6_%' THEN 'Bridge Division'
            ELSE CONCAT(DISTRICT_DIVISION_ABBR, ' - ', DISTRICT_MPO_NEW)
        END AS District_MPO_Division,
        METRIC_VALUE
    FROM co_update_mpo
)

SELECT DISTINCT
    DISTRICT,
    District_MPO_Division,
    CATEGORY_NEW AS CATEGORY,
    METRIC_NAME,
    METRIC_VALUE AS CHANGE_ORDERS_AMOUNT
FROM change_orders_final
;