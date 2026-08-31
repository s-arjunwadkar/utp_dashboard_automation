USE DATABASE UTP_DASHBOARD;
USE WAREHOUSE UTP_DASHBOARD_WH;

CREATE OR REPLACE VIEW SILVER.NORMAL_TARGETS
COMMENT = 'These are the records from targets and carryover file where the organization scope matches the expected scope.'
AS
SELECT DISTINCT
        category,
        district_mpo_division,
        fy,
        carryovers,
        total_targets,
        org_type,
        expected_org_type
FROM SILVER.TARGETS_SCOPE
WHERE LOWER(org_type) = LOWER(expected_org_type)
    OR (category = '11DD' AND LOWER(org_type) = 'statewide' AND LOWER(expected_org_type) = 'district')
    OR (category ='6' AND LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'statewide')
    OR (category ='9' AND LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'division')
    OR (category ='10CR' AND LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'statewide')
ORDER BY category, district_mpo_division, fy;

CREATE OR REPLACE VIEW SILVER.EXCEPTION_TARGETS
COMMENT = 'This table contains records from targets and carryover file where there are some exceptions to the expected organization scope of the category.'
AS
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
  WHERE (category IN ('12CL', '12TTC', '12OTHER')
      AND (LOWER(org_type) IN ('other', 'statewide') AND LOWER(expected_org_type) = 'district'))
      OR (category = '4R'
      AND (LOWER(org_type) = 'statewide' AND LOWER(expected_org_type) = 'district'))
  GROUP BY category, district_mpo_division, fy, org_type, expected_org_type
),

cat_10_11_8_subprograms_table AS (
  SELECT DISTINCT 
    category,
    district_mpo_division,
    fy,
    SUM(carryovers) AS carryovers,
    SUM(total_targets) AS total_targets,
    org_type,
    expected_org_type
  FROM exceptions_table
  WHERE (category IN ('10ADA', '10FB', '10GR', '10LIA', '10RGC', '10RGS', '10TPW', '10SRATP', '10SCP', '10ITD', '10EAR', '10OTHER')
      AND (LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'district')) 
      OR (category IN ('11B', '11CO') AND (LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'district'))
      OR (category IN ('8SF', '8RX') AND (LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'district'))
  GROUP BY category, district_mpo_division, fy, org_type, expected_org_type
),

cat_10cr_table AS (
    SELECT DISTINCT
        *
    FROM exceptions_table
    WHERE category = '10CR' 
        AND (LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'statewide')
)

SELECT * FROM simple_table
UNION ALL
SELECT * FROM cat_4r_12_table
UNION ALL
SELECT * FROM cat_10_11_8_subprograms_table
UNION ALL
SELECT * FROM cat_10cr_table
ORDER BY category, district_mpo_division, fy
;