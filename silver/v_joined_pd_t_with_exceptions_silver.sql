USE DATABASE UTP_DASHBOARD;
USE WAREHOUSE UTP_DASHBOARD_WH;

CREATE OR REPLACE VIEW SILVER.TOTAL_WITH_EXCPN_VIEW
COMMENT = 'This tables appends the exceptions in target file to the joined project details and targets table.'
AS
WITH exceptions AS ( 
SELECT
    category,
    district_mpo_division,
    fy::INTEGER AS fy,
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
),

targets_by_district_4r_8_12_10_11s AS (
    SELECT
        category,
        district_mpo_division,
        fy,
        total_authorized_amount,
        CASE
            WHEN category IN ('4R', '12') AND district_mpo_division NOT ILIKE 'Statewide%' THEN total_authorized_amount
            WHEN category IN ('10ADA', '10FB', '10GR', '10LIA', '10RGC', '10RGS', '10TPW', '10SRATP', '10SCP', '10IDA', '10EAR', '10OTHER') AND district_mpo_division NOT ILIKE '10 - %' THEN total_authorized_amount
            WHEN category = '11B' AND district_mpo_division NOT ILIKE 'Rider%' THEN total_authorized_amount
            WHEN category = '11CO' AND district_mpo_division NOT ILIKE '%Overruns%' THEN total_authorized_amount
            WHEN category = '8SF' AND district_mpo_division NOT ILIKE '8 - Safety' THEN total_authorized_amount
            WHEN category = '8RX' AND district_mpo_division NOT ILIKE '8 - Rail' THEN total_authorized_amount
            ELSE total_targets
        END AS total_targets,
        carryovers
    FROM normal_exceptions_union
),

target_totals_4r_8_12_10_11s AS (
    SELECT
        category,
        fy,
        SUM(total_targets) AS new_targets_yearly
    FROM targets_by_district_4r_8_12_10_11s
    WHERE (category IN ('4R', '12') AND district_mpo_division NOT ILIKE 'Statewide%')
        OR (category IN ('10ADA', '10FB', '10GR', '10LIA', '10RGC', '10RGS', '10TPW', '10SRATP', '10SCP', '10IDA', '10EAR', '10OTHER') AND district_mpo_division NOT ILIKE '10 - %')
        OR (category = '11B' AND district_mpo_division NOT ILIKE 'Rider%')
        OR (category = '11CO' AND district_mpo_division NOT ILIKE '%Overruns%')
        OR (category = '8SF' AND district_mpo_division NOT ILIKE '8 - Safety')
        OR (category = '8RX' AND district_mpo_division NOT ILIKE '8 - Rail')
    GROUP BY category, fy
    ORDER BY category, fy
),

adjust_4r_8_12_10_11s AS (
    SELECT
        m.category,
        m.district_mpo_division,
        m.fy,
        m.total_authorized_amount,
        m.total_targets,
        s.new_targets_yearly,
        CASE
            WHEN district_mpo_division ILIKE 'Statewide%' AND m.category = '4R' AND (m.category = s.category) AND (m.fy = s.fy) THEN m.total_targets - s.new_targets_yearly
            WHEN district_mpo_division = 'Statewide Strategic Priority' AND m.category = '12' AND (m.category = s.category) AND (m.fy = s.fy) THEN m.total_targets - s.new_targets_yearly
            WHEN district_mpo_division ILIKE '10 - %' AND m.category IN ('10ADA', '10FB', '10GR', '10LIA', '10RGC', '10RGS', '10TPW', '10SRATP', '10SCP', '10IDA', '10EAR', '10OTHER') AND (m.category = s.category) AND (m.fy = s.fy) THEN m.total_targets - s.new_targets_yearly
            WHEN district_mpo_division ILIKE 'Rider%' AND m.category = '11B' AND (m.category = s.category) AND (m.fy = s.fy) THEN m.total_targets - s.new_targets_yearly
            WHEN district_mpo_division ILIKE '%Overruns%' AND m.category = '11CO' AND (m.category = s.category) AND (m.fy = s.fy) THEN m.total_targets - s.new_targets_yearly
            WHEN district_mpo_division ILIKE '8 - Safety' AND m.category = '8SF' AND (m.category = s.category) AND (m.fy = s.fy) THEN m.total_targets - s.new_targets_yearly
            WHEN district_mpo_division ILIKE '8 - Rail' AND m.category = '8RX' AND (m.category = s.category) AND (m.fy = s.fy) THEN m.total_targets - s.new_targets_yearly
            ELSE m.total_targets
        END AS total_targets_new,
        m.carryovers
    FROM targets_by_district_4r_8_12_10_11s AS m
    LEFT JOIN target_totals_4r_8_12_10_11s AS s
    ON m.category = s.category AND m.fy = s.fy
)

SELECT
    category,
    district_mpo_division,
    fy,
    total_authorized_amount,
    total_targets_new AS total_targets,
    carryovers,
    total_targets_new + carryovers AS targets_carryovers_combined
FROM adjust_4r_8_12_10_11s
ORDER BY category, district_mpo_division, fy
;