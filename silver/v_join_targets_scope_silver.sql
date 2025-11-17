USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE VIEW SILVER.TARGETS_SCOPE 
COMMENT = 'Remove Carryover records for FY other than 2026, Add organization scope to the file for each category' 
AS
SELECT 
    tt.category,
    tt.district_mpo_division,
    tt.fy,
    CASE
        WHEN tt.fy = '2026' THEN tt.carryovers
        ELSE 0
    END AS carryovers,
    tt.total_targets,
    CASE
        WHEN UPPER(RIGHT(TRIM(tt.district_mpo_division), 3)) = 'MPO' THEN 'MPO'
        WHEN REGEXP_LIKE(TRIM(tt.district_mpo_division), '^[A-Z]{3}\\s-\\s.+$', 'c') THEN 'DISTRICT'
        WHEN TRIM(tt.district_mpo_division) ILIKE '%statewide%' THEN 'STATEWIDE'
        ELSE 'OTHER'
      END AS org_type,
    cm.org_scope AS expected_org_type
FROM SILVER.TOTAL_TARGETS AS tt
LEFT JOIN REF.V_CATEGORY_MAP_CURRENT AS cm
ON tt.category = cm.new_category
;
-- SELECT * FROM SILVER.TARGETS_SCOPE;