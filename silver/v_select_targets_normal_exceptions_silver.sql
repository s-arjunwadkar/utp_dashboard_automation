USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

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
    OR (category ='11' AND LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'division')
    OR (category ='6' AND LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'statewide')
    OR (category ='8' AND LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'statewide')
    OR (category ='9' AND LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'division')
    OR (category ='10' AND LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'statewide')
    OR (category ='10CR' AND LOWER(org_type) = 'other' AND LOWER(expected_org_type) = 'statewide')
ORDER BY category, district_mpo_division, fy;

CREATE OR REPLACE VIEW SILVER.EXCEPTION_TARGETS
COMMENT = 'This table contains records from targets and carryover file where there are some exceptions to the expected organization scope of the category.'
AS
WITH exceptions_table AS (
    SELECT DISTINCT
        category,
        district_mpo_division,
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
    WHERE category IN ('2', '4R', '4U', '11ES')
        AND (carryovers <> 0
        OR total_targets <> 0)
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
SELECT * FROM cat_10cr_table
ORDER BY category, district_mpo_division
;

-- SELECT * FROM SILVER.NORMAL_TARGETS;
-- SELECT * FROM SILVER.EXCEPTION_TARGETS;