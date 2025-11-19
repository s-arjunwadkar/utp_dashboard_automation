USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

-- SELECT * FROM BRONZE.CARRYOVERS_TARGETS_V38
-- WHERE CATEGORY = '2';

-- SELECT * FROM REF.V_TARGETS_ORG_CANON;

-- SELECT * FROM REF.V_TARGETS_ORG_CANON
-- WHERE RAW_DISPLAY ILIKE 'Statewide - Cat%';

-- SELECT pd.new_category AS category, pd.district_division_abbr, pd.district_division, ct.district_mpo_division, pd.mpo_description 
-- FROM SILVER.V_PD_WITH_NEW_CATEGORY AS pd
-- INNER JOIN BRONZE.CARRYOVERS_TARGETS_V38 AS ct
-- ON pd.new_category = ct.category
-- WHERE pd.new_category = '2';

-- 1) Unpivot Target for FY & Calculate Total Target per year per cat
-- WITH unpivot_targets AS (
--     SELECT *
--     FROM BRONZE.CARRYOVERS_TARGETS_V38
--     UNPIVOT (targets FOR FY IN (fy_2026, fy_2027, fy_2028, fy_2029, fy_2030, fy_2031, fy_2032, fy_2033, fy_2034, fy_2035))
--     ORDER BY CATEGORY
-- ),

-- formatted_targets AS (
-- SELECT
--     category,
--     district_mpo_division,
--     carryovers,
--     TRIM(SPLIT_PART(FY, '_', 2)) AS FY,
--     targets
-- FROM unpivot_targets
-- )

-- SELECT
--     category,
--     district_mpo_division,
--     fy,
--     carryovers,
--     SUM(targets) AS total_targets
-- FROM formatted_targets
-- GROUP BY category, district_mpo_division, carryovers, fy
-- ORDER BY category, district_mpo_division, fy
-- ;
-- -- 2) Targets + Org_type
-- SELECT 
--     tt.category,
--     tt.district_mpo_division,
--     tt.fy,
--     CASE
--         WHEN tt.fy = '2026' THEN tt.carryovers
--         ELSE 0
--     END AS carryovers,
--     tt.total_targets,
--     CASE
--         WHEN UPPER(RIGHT(TRIM(tt.district_mpo_division), 3)) = 'MPO' THEN 'MPO'
--         WHEN REGEXP_LIKE(TRIM(tt.district_mpo_division), '^[A-Z]{3}\\s-\\s.+$', 'c') THEN 'DISTRICT'
--         WHEN TRIM(tt.district_mpo_division) ILIKE '%statewide%' THEN 'STATEWIDE'
--         ELSE 'OTHER'
--       END AS org_type,
--     cm.org_scope AS expected_org_type
-- FROM SILVER.TOTAL_TARGETS AS tt
-- LEFT JOIN REF.V_CATEGORY_MAP_CURRENT AS cm
-- ON tt.category = cm.new_category
-- WHERE tt.district_mpo_division <> 'TOTAL'
-- ;

-- -- 2A) Create a seprate table to store records which are an exception.
-- WITH exceptions_table AS (
--     SELECT DISTINCT
--         category,
--         district_mpo_division,
--         carryovers,
--         total_targets,
--         org_type,
--         expected_org_type
--     FROM SILVER.TARGETS_SCOPE
--     WHERE LOWER(org_type) <> LOWER(expected_org_type) 
-- ),

-- simple_table AS (
--     SELECT DISTINCT *
--     FROM exceptions_table
--     WHERE category IN ('2', '4R', '4U', '9', '11ES')
--         AND (carryovers <> 0
--         OR total_targets <> 0)
-- ),

-- cat_10cr_table AS (
--     SELECT DISTINCT
--         *
--     FROM exceptions_table
--     WHERE category = '10CR' 
--         AND (LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'statewide')
-- )

-- SELECT * FROM simple_table
-- UNION ALL
-- SELECT * FROM cat_10cr_table
-- ORDER BY category, district_mpo_division
-- ;


-- SELECT DISTINCT
--         category,
--         district_mpo_division,
--         fy,
--         carryovers,
--         total_targets,
--         org_type,
--         expected_org_type
--     FROM SILVER.TARGETS_SCOPE
--     WHERE LOWER(org_type) = LOWER(expected_org_type)
--         OR (category ='11' AND LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'division')
--         OR (category = '8' AND TRIM(district_mpo_division) ILIKE 'Federal Railway-Highway Safety Program')
--     ORDER BY category, district_mpo_division, fy;

/*
org_type vs expected_org_type = FALSE
2 -> 'keep' records (carryovers)
4R -> 'keep' records (carryovers)(keep for now, later logic review coming)
4U -> 'keep' records (carryovers)(duplicates, keep only unique)
8 -> 'keep' records; Federal Railway-Highway Safety Program is the only record and has target values in each year (BETTER TO HANDLE IT IN THE START)(DONE!!)
9 -> 'keep' records; (I THINK CAN BE HANDLED IN THE START)(PENDING!!)
10CR -> 'drop' records 'except' carbon reduc..FLEX.. other vs statewide unique records
11 -> Other vs Division 'keep' unique records (I see deplicates) (HANDLE AT THE START)(DONE!!)
11 -> Other vs District 'drop' records
11 -> Statewide (carryovers, all 0) 'drop' records (for now but make a note)
11 -> District vs Division 'drop' records
11ES -> 'Keep' records (carryovers)
11SF -> Statewide (carryovers, all 0) 'drop' records (for now but make a note)
12 -> Statewide vs district 'drop' for now until 12 finailize
*/

-- -- 3) Project Details Get MPO_shorts then display names
-- SELECT
--       pd.*,
--       mpo.mpo_short
-- FROM SILVER.V_PD_WITH_NEW_CATEGORY AS pd
-- LEFT JOIN REF.MPO_REFERENCE AS mpo
-- ON TRIM(pd.mpo_description) = TRIM(mpo.mpo_description)
-- WHERE pd.new_category IN ('1','2')

-- -- 4) Separate PD for Total Authorized amount per category per estimated fy with display names
-- WITH cat_1_2_pd AS (
--     SELECT *
--     FROM SILVER.PD_MPO_SHORT
--     WHERE category IN ('1', '2')
-- ),

-- pd_main AS (
-- SELECT
--     parent_category, 
--     category, 
--     district_division_abbr,
--     district_division,
--     mpo_short,
--     CASE
--         WHEN LOWER(org_scope) = 'district' THEN CONCAT(district_division_abbr, ' - ', district_division)
--         WHEN LOWER(org_scope) = 'mpo' AND mpo_short IS NOT NULL THEN CONCAT(district_division_abbr, ' - ', mpo_short)
--         ELSE district_division
--     END AS district_mpo_division,
--     estimated_fiscal_year, 
--     authorized_amount,
--     org_scope
-- FROM cat_1_2_pd
-- )

-- SELECT
--     parent_category,
--     category,
--     district_mpo_division,
--     estimated_fiscal_year,
--     SUM(authorized_amount) AS total_authorized_amount
-- FROM pd_main
-- GROUP BY parent_category, category, district_mpo_division, estimated_fiscal_year
-- ORDER BY category, district_mpo_division, estimated_fiscal_year
-- ;
-- -- 5) Join with targets to get total targets and carryovers
-- WITH targets AS (
--     SELECT 
--         *
--     FROM SILVER.NORMAL_TARGETS
--     WHERE category IN ('1', '2')
-- )

-- SELECT 
--     COALESCE(pd.category, tg.category)::STRING AS category, 
--     COALESCE(pd.district_mpo_division, tg.district_mpo_division)::STRING AS district_mpo_division, 
--     COALESCE(pd.estimated_fiscal_year, tg.fy)::INTEGER AS fy, 
--     COALESCE(pd.total_authorized_amount, 0)::FLOAT AS total_authorized_amount, 
--     COALESCE(tg.total_targets, 0)::FLOAT AS total_targets, 
--     COALESCE(tg.carryovers, 0)::FLOAT AS carryovers
-- FROM SILVER.PD_TOTAL_PROGRAMMED AS pd
-- FULL OUTER JOIN targets AS tg
-- ON pd.category = tg.category AND pd.district_mpo_division = tg.district_mpo_division AND pd.estimated_fiscal_year = tg.fy
-- ORDER BY category, district_mpo_division, fy
-- ;

-- -- 3) Build PD display_name (special MPO cases first)
-- pd_final AS (
--   SELECT
--       pdm.*,
--       CASE
--         WHEN pdm.org_scope = 'MPO' AND pdm.mpo_short = 'HGAC MPO'
--           THEN CONCAT('HOU/BMT',' - ', pdm.mpo_short)
--         WHEN pdm.org_scope = 'MPO' AND pdm.mpo_short = 'NCTCOG MPO'
--           THEN CONCAT('DAL/FTW/PAR',' - ', pdm.mpo_short)
--         WHEN pdm.org_scope = 'MPO' AND pdm.mpo_short NOT IN ('NCTCOG MPO', 'HGAC MPO')
--           THEN CONCAT(pdm.district_division_abbr,' - ', pdm.mpo_short)
--         ELSE CONCAT(pdm.district_division_abbr, ' - ', pdm.district_division)
--       END AS display_name
--   FROM pd_mpo_joined AS pdm
-- ),

-- Add the exceptions to the joined total
-- WITH exceptions AS ( 
-- SELECT
--     category,
--     district_mpo_division,
--     2026 AS fy,
--     0 AS total_authorized,
--     total_targets,
--     carryovers
-- FROM SILVER.EXCEPTION_TARGETS
-- WHERE category IN ('2')
-- )

-- SELECT * FROM SILVER.JOINED_PD_TARGET_VIEW
-- UNION ALL
-- SELECT * FROM exceptions
-- ORDER BY category, district_mpo_division, fy
-- ;

-- Another way to handle special character from pd mpo
-- CREATE OR REPLACE VIEW REF.MPO_COPY
-- COMMENT = 'This is a copy table to handle the special character in project details mpo in another way.'
-- AS
-- WITH pd_mpos AS (
-- SELECT 
--     DISTINCT mpo_description 
-- FROM SILVER.V_PD_WITH_NEW_CATEGORY 
-- WHERE mpo_description IS NOT NULL
-- ),

-- new_mpo_ref AS (
-- SELECT 
--     COALESCE (mp.mpo_description, pd.mpo_description) AS mpo_description,
--     mp.mpo_short
-- FROM pd_mpos AS pd
-- FULL OUTER JOIN REF.MPO_REFERENCE AS mp
-- ON pd.mpo_description = mp.mpo_description
-- )

-- SELECT
--     mpo_description,
--     CASE
--         WHEN mpo_description ILIKE '%south%' THEN 'SETRPC MPO'
--         ELSE mpo_short
--     END AS mpo_short
-- FROM new_mpo_ref
-- ;


-- SELECT * FROM SILVER.TOTAL_TARGETS
-- WHERE category ILIKE '%11%';

-- SELECT * FROM SILVER.TARGETS_SCOPE
-- WHERE category ILIKE '%11%';

-- SELECT * FROM SILVER.NORMAL_TARGETS
-- WHERE category ILIKE '%11%';

-- SELECT * FROM SILVER.EXCEPTION_TARGETS
-- WHERE category ILIKE '%11%';

-- SELECT * FROM SILVER.PD_MPO_SHORT
-- WHERE category ILIKE '%11%';

-- WITH cat_select_pd AS (
--     SELECT *
--     FROM SILVER.PD_MPO_SHORT
--     --WHERE category IN ('1', '2', '4R', '4U', '5', '6', '7', '8', '9', '10', '10CR', '11', '11ES', '11SF', '12', 'DA')
--     --    AND NOT (category = '11' AND work_program_code = '2910GR') -- Should be CAT 10/ Fix the Work Program (BY TxDOT)
--     WHERE NOT (category = '11' AND work_program_code = '2910GR') -- Should be CAT 10/ Fix the Work Program (BY TxDOT)
-- ),

WITH pd_main AS (
SELECT
    csj,
    parent_category, 
    category, 
    district_division_abbr,
    district_division,
    mpo_short,
    CASE
        WHEN LOWER(org_scope) = 'district' THEN CONCAT(district_division_abbr, ' - ', district_division)
        WHEN LOWER(org_scope) = 'mpo' AND mpo_short IS NOT NULL THEN CONCAT(district_division_abbr, ' - ', mpo_short)
        WHEN category = '6' THEN 'Bridge Division'
        WHEN category = '8' THEN 'Traffic Division'
        WHEN category = '9' AND (work_program_code ILIKE '%FX' OR pid_code IN ('BRA', 'TE', 'SRS')) THEN 'Transportation Alternatives Flex Program'
        WHEN category = '9' AND work_program_code ILIKE '%JA' THEN 'Transportation Alternatives Flex IIJA Program'
        WHEN category = '9' AND work_program_code ILIKE '%TP' AND (pid_code != 'TM' OR pid_code IS NULL) THEN 'Transportation Alternatives Program - Non-TMAs'
        WHEN category = '10' THEN 'Supplemental Transportation Projects'
        WHEN category = '10CR' AND work_program_code = '10CBNS' THEN 'Carbon Reduction Program - Statewide'
        WHEN category = '11' AND work_program_code = '16B11' THEN 'Rider 11B Program'
        WHEN category = '11' AND work_program_code = 'COCO' THEN 'Cost Overruns/Change Orders'
        ELSE district_division
    END AS district_mpo_division,
    work_program_code,
    pid_code,
    estimated_fiscal_year, 
    authorized_amount,
    org_scope
FROM SILVER.PD_MPO_SHORT
WHERE NOT (category = '11' AND work_program_code = '2910GR') -- Should be CAT 10/ Fix the Work Program (BY TxDOT)
),

pd_final AS (
    SELECT
        *,
        CASE
        WHEN district_mpo_division IN ('BMT - HGAC MPO', 'HOU - HGAC MPO')
          THEN 'HOU/BMT - HGAC MPO'
        WHEN district_mpo_division IN ('DAL - NCTCOG MPO', 'FTW - NCTCOG MPO', 'PAR - NCTCOG MPO')
          THEN 'DAL/FTW/PAR - NCTCOG MPO'
        ELSE district_mpo_division
      END AS display_name
    FROM pd_main
)
SELECT * FROM pd_final
ORDER BY category, display_name, estimated_fiscal_year;

SELECT * FROM SILVER.TOTAL_WITH_EXCPN_VIEW;
SELECT * FROM SILVER.JOINED_PD_TARGET_VIEW;
SELECT * FROM SILVER.PD_TOTAL_PROGRAMMED;

SELECT * FROM SILVER.EXCEPTION_TARGETS;


WITH exceptions_table AS (
    SELECT DISTINCT
        category,
        district_mpo_division,
        fy,
        carryovers,
        total_targets,
        org_type,
        expected_org_type
    FROM SILVER.TARGETS_SCOPE
    WHERE LOWER(org_type) <> LOWER(expected_org_type) 
),

simple_table AS (
    SELECT DISTINCT *
    FROM exceptions_table
    WHERE category IN ('2', '4U', '11ES')
        AND (carryovers <> 0
        OR total_targets <> 0)
),

cat_4r_12_table AS (
  SELECT DISTINCT 
    category,
    district_mpo_division,
    fy,
    SUM(carryovers) AS carryovers,
    SUM(total_targets) AS total_targets,
    org_type,
    expected_org_type
  FROM exceptions_table
  WHERE category IN ('4R', '12')
      AND (LOWER(org_type) = 'statewide' AND LOWER(expected_org_type) = 'district')
  GROUP BY category, district_mpo_division, fy, org_type, expected_org_type
),

cat_10cr_table AS (
    SELECT DISTINCT
        *
    FROM exceptions_table
    WHERE category = '10CR' 
        AND (LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'statewide')
),

union_tables AS (
  SELECT * FROM simple_table
  UNION ALL
  SELECT * FROM cat_4r_12_table
  UNION ALL
  SELECT * FROM cat_10cr_table
  ORDER BY category, district_mpo_division
)

SELECT
    category,
    district_mpo_division,
    fy,
    0 AS total_authorized,
    total_targets,
    carryovers
FROM union_tables
;