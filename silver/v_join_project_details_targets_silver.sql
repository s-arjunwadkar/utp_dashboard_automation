USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;


CREATE OR REPLACE VIEW SILVER.JOINED_PD_TARGET_VIEW
COMMENT = 'This table joins both project details and carryover and targets file to get total programmed amount, total target amounts and carryover amounts for each category, district_mpo_division and fy.'
AS
WITH targets AS (
    SELECT 
        *
    FROM SILVER.NORMAL_TARGETS
    WHERE category IN ('1', '2', '4U', '5', '6', '7', '8', '9', '10', '10CR', '11', '11ES', '11SF')
)

SELECT 
    COALESCE(pd.category, tg.category)::STRING AS category, 
    COALESCE(pd.district_mpo_division, tg.district_mpo_division)::STRING AS district_mpo_division, 
    COALESCE(pd.estimated_fiscal_year, tg.fy)::INTEGER AS fy, 
    COALESCE(pd.total_authorized_amount, 0)::FLOAT AS total_authorized_amount, 
    COALESCE(tg.total_targets, 0)::FLOAT AS total_targets, 
    COALESCE(tg.carryovers, 0)::FLOAT AS carryovers
FROM SILVER.PD_TOTAL_PROGRAMMED AS pd
FULL OUTER JOIN targets AS tg
ON pd.category = tg.category AND pd.district_mpo_division = tg.district_mpo_division AND pd.estimated_fiscal_year = tg.fy
ORDER BY category, district_mpo_division, fy
;

-- SELECT * FROM SILVER.JOINED_PD_TARGET_VIEW;