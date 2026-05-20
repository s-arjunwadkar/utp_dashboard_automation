-- TRUNCATE TABLE REF.CATEGORY_MAP;
USE DATABASE UTP_DASHBOARD;
USE WAREHOUSE UTP_DASHBOARD_WH;

INSERT INTO REF.CATEGORY_MAP
(category_parent, work_program_exact, work_program_regex, pid_exact, pid_regex,
 new_category, org_scope, comments)

-- 1 — District
VALUES ('1',  NULL, NULL, NULL, NULL, '1',    'District', 'Cat 1 ⇒ District'),

-- 2 — MPO (mpo_description may be null in PD; we’ll handle that later)
       ('2',  NULL, NULL, NULL, NULL, '2',    'MPO',      'Cat 2 ⇒ MPO'),

-- 4 — split by Work Program
       ('4',  '04CN',  NULL, NULL, NULL, '4R', 'District', '4→4R when WP=04CN'),
       ('4',  '043C',  NULL, NULL, NULL, '4U', 'District', '4→4U when WP=043C'),
       ('4',  '1904U', NULL, NULL, NULL, '4U', 'District', '4→4U when WP=1904U'),

-- 5 — MPO
       ('5',  NULL, NULL, NULL, NULL, '5',    'MPO',       'Cat 5 ⇒ MPO'),

-- 6 — Statewide / Division (Bridge)
       ('6',  NULL, NULL, NULL, NULL, '6',    'Statewide',    'Cat 6 ⇒ Division/Statewide'),

-- 7 — MPO
       ('7',  NULL, NULL, NULL, NULL, '7',    'MPO',       'Cat 7 ⇒ MPO'),

-- 8 — Statewide / Division (Traffic)
       ('8',  NULL, NULL, NULL, NULL, '8',    'Statewide',    'Cat 8 ⇒ Division/Statewide'),

-- 9 — MPO for specific Work Programs; PTN TASA; TASA Flex; TASA Flex IIJA
       ('9', NULL, '.*09$', NULL, NULL, '9',  'MPO', 'Any WP ending with 09'),
       ('9', NULL, '.*FX$', NULL, NULL, '9',  'Division', 'Any WP ending with FX ⇒ TASA Flex'), 
       ('9', NULL, NULL, 'BRA', NULL, '9',  'Division', 'PID with BRA ⇒ TASA Flex'),
       ('9', NULL, NULL, 'TE', NULL, '9',  'Division', 'PID with TE ⇒ TASA Flex'),
       ('9', NULL, NULL, 'SRS', NULL, '9',  'Division', 'PID with SRS ⇒ TASA Flex'),
       ('9', NULL, '.*JA$', NULL, NULL, '9',  'Division', 'Any WP ending with JA ⇒ TASA Flex IIJA'),
       ('9', NULL, '.*TP$', NULL, NULL, '9',  'Division', 'Any WP ending with TP and PID is TP or PID <> TM or is NULL ⇒ PTN TASA'),
       ('9', 'FRH09M', NULL, NULL, NULL, '9',  'MPO', 'A new work program is added.'),
       
-- 10 — 10CR splits + temporary default “rest = Statewide”
       ('10', '10CBNM', NULL, NULL, NULL, '10CR', 'MPO',    '10CR MPO program'),
       ('10', '10CBNS', NULL, NULL, NULL, '10CR', 'Statewide', '10CR Statewide program'),
       ('10', NULL, NULL, 'ADA', NULL, '10ADA',  'District', '10 - Americans with Disabilities Act => Subcategory of 10.'),
       ('10', NULL, NULL, 'FB', NULL, '10FB',  'District', '10 - Ferry Program => Subcategory of 10.'),
       ('10', NULL, NULL, 'GR', NULL, '10GR',  'District', '10 - Green Ribbon Program => Subcategory of 10.'),
       ('10', NULL, NULL, 'LIA', NULL, '10LIA',  'District', '10 - Landscape Incentive Awards Program => Subcategory of 10.'),
       ('10', NULL, NULL, 'RGC', NULL, '10RGC',  'District', '10 - Railroad Grade Crossing Program => Subcategory of 10.'),
       ('10', NULL, NULL, 'RGS', NULL, '10RGS',  'District', '10 - Railroad Signal Maintenance Program => Subcategory of 10.'),
       ('10', NULL, NULL, 'TPW', NULL, '10TPW',  'District', '10 - Texas Parks and Wildlife Program => Subcategory of 10.'),
       ('10', 'SRATP', NULL, NULL, NULL, '10SRATP',  'District', '10 - Safety Rest Area Truck Parking => Subcategory of 10.'),
       ('10', 'SCPON', NULL, NULL, NULL, '10SCP',  'District', '10 - Seaport Connectivity Program => Subcategory of 10.'),
       ('10', NULL, NULL, NULL, 'ITD', '10ITD',  'District', '10 - Intelligent Transportation Systems => Subcategory of 10.'),
       ('10', NULL, NULL, NULL, 'DMO', '10EAR',  'District', '10 - Federal Earmarks => Subcategory of 10.'),
       ('10', NULL, NULL, NULL, 'HPS', '10EAR',  'District', '10 - Federal Earmarks => Subcategory of 10.'),
       ('10', NULL, NULL, NULL, 'PRO', '10EAR',  'District', '10 - Federal Earmarks => Subcategory of 10.'),
       ('10', NULL, NULL, NULL, 'HIP', '10EAR',  'District', '10 - Federal Earmarks => Subcategory of 10.'),
       ('10', NULL, NULL, NULL, 'PLH', '10EAR',  'District', '10 - Federal Earmarks => Subcategory of 10.'),
       ('10', NULL, NULL, NULL, 'FIP', '10EAR',  'District', '10 - Federal Earmarks => Subcategory of 10.'),
       ('10', NULL, NULL, NULL, 'BLD', '10EAR',  'District', '10 - Federal Earmarks => Subcategory of 10.'),
       ('10', NULL, NULL, NULL, 'RAI', '10EAR',  'District', '10 - Federal Earmarks => Subcategory of 10.'),
       ('10', NULL, NULL, NULL, 'INF', '10EAR',  'District', '10 - Federal Earmarks => Subcategory of 10.'),
       ('10', NULL, NULL, NULL, NULL, '10OTHER',  'District', 'All the other remaining programs, 10 - Federal Lands Access Program => Subcategory of 10.'),

-- 11 — subcats + generic “ends with 11”
       ('11', '11SF',   NULL, NULL, NULL, '11SF','District','11SF ⇒ District'),
       ('11', '11PES',  NULL, NULL, NULL, '11ES','District','11ES ⇒ District'),
       ('11', '16B11', NULL, NULL, NULL, '11B',  'District', 'Rider 11B Program => Subcategory of 11. Border State Infrastructure.'),
       ('11', 'COCO', NULL, NULL, NULL, '11CO',  'District', 'Change Orders/Cost Overruns => Subcategory of 11.'),
       ('11', NULL, '.*11$', NULL, NULL, '11DD',  'District', 'Any WP ending with 11. District Discretionary Cat 11 Subcategory'),

-- 12 — default District (until clarified)
       ('12', NULL, NULL, NULL, NULL, '12', 'District', 'Cat 12 default'),

-- DA — default District
       ('DA', NULL, NULL, NULL, NULL, 'DA', 'District', 'DA default to District');

-- -- 9 — MPO for specific Work Programs; PTN TASA; TASA Flex; TASA Flex IIJA
-- -- First need to mark pervious record as in active as of today
-- UPDATE REF.CATEGORY_MAP
-- SET is_active = FALSE,
--     valid_to = CURRENT_TIMESTAMP()
-- WHERE category_parent = '9';

-- -- Now lets Insert new logic
-- INSERT INTO REF.CATEGORY_MAP
-- (category_parent, work_program_exact, work_program_regex, pid_exact, pid_regex,
--  new_category, org_scope, comments)
-- VALUES ('9', NULL, '.*09$', NULL, NULL, '9',  'MPO', 'Any WP ending with 09'),
--        ('9', NULL, '.*FX$', NULL, NULL, '9',  'Division', 'Any WP ending with FX ⇒ TASA Flex'), 
--        ('9', NULL, NULL, 'BRA', NULL, '9',  'Division', 'PID with BRA ⇒ TASA Flex'),
--        ('9', NULL, NULL, 'TE', NULL, '9',  'Division', 'PID with TE ⇒ TASA Flex'),
--        ('9', NULL, NULL, 'SRS', NULL, '9',  'Division', 'PID with SRS ⇒ TASA Flex'),
--        ('9', NULL, '.*JA$', NULL, NULL, '9',  'Division', 'Any WP ending with JA ⇒ TASA Flex IIJA'),
--        ('9', NULL, '.*TP$', NULL, NULL, '9',  'Division', 'Any WP ending with TP and PID is TP or PID <> TM or is NULL ⇒ PTN TASA');
       
-- SELECT * FROM REF.CATEGORY_MAP;
-- DROP TABLE IF EXISTS REF.CATEGORY_MAP;

-- INSERT INTO REF.CATEGORY_MAP
-- (category_parent, work_program_exact, work_program_regex, pid_exact, pid_regex,
--  new_category, org_scope, comments, is_active, valid_from, valid_to)
-- VALUES ('9', 'FRH09M', NULL, NULL, NULL, '9',  'MPO', 'A new work program is added.', TRUE, CURRENT_TIMESTAMP(), NULL);