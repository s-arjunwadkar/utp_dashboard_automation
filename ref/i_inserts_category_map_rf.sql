-- TRUNCATE TABLE REF.CATEGORY_MAP;
USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

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
       
-- 10 — 10CR splits + temporary default “rest = Statewide”
       ('10', '10CBNM', NULL, NULL, NULL, '10CR', 'MPO',    '10CR MPO program'),
       ('10', '10CBNS', NULL, NULL, NULL, '10CR', 'Statewide', '10CR Statewide program'),
       ('10', NULL,     NULL, NULL, NULL, '10',   'Statewide', 'Cat 10 default for now'),

-- 11 — subcats + generic “ends with 11”
       ('11', '11SF',   NULL, NULL, NULL, '11SF','District','11SF ⇒ District'),
       ('11', '11PES',  NULL, NULL, NULL, '11ES','District','11ES ⇒ District'),
       ('11', '16B11',  NULL, NULL, NULL, '11',  'Division','Rider 11B ⇒ Division'),
       ('11', 'COCO',   NULL, NULL, NULL, '11',  'Division','Change Orders ⇒ Division'),
       ('11', NULL, '.*11$', NULL, NULL, '11',  'District','Any WP ending with 11'),

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