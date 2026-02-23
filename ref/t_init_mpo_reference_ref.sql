USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE SCHEMA IF NOT EXISTS REF;

CREATE OR REPLACE TABLE REF.MPO_REFERENCE (
  MPO_DESCRIPTION STRING NOT NULL,
  MPO_SHORT       STRING NOT NULL,
  DISTRICT        STRING NOT NULL,
  CONSTRAINT MPO_REFERENCE_UQ UNIQUE (MPO_DESCRIPTION, DISTRICT)  -- logical uniqueness
)
COMMENT = 'This table stores mpo names, their long and short versions. Used as reference to standardize names.';

TRUNCATE TABLE IF EXISTS REF.MPO_REFERENCE;

-- Seed rows (idempotent-ish; will error on exact-duplicate MPO_DESCRIPTION because of the UNIQUE)
INSERT INTO REF.MPO_REFERENCE (MPO_DESCRIPTION, MPO_SHORT, DISTRICT)
SELECT * FROM VALUES
  ('Abilene Metropolitan Planning Organization',                'Abilene MPO', 'Abilene'),
  ('Alamo Area Metropolitan Planning Organization',             'AAMPO', 'San Antonio'),
  ('Amarillo Metropolitan Planning Organization',               'Amarillo MPO', 'Amarillo'),
  ('Bryan-College Station Metropolitan Planning Organization',  'Bryan-College Station MPO', 'Bryan'),
  ('Capital Area Metropolitan Planning Organization',           'CAMPO MPO', 'Austin'),
  ('Corpus Christi Metropolitan Planning Organization',         'Corpus Christi MPO', 'Corpus Christi'),
  ('Eagle Pass Metropolitan Planning Organization',             'Eagle Pass MPO', 'Laredo'),
  ('El Paso Metropolitan Planning Organization',                'El Paso MPO', 'El Paso'),
  ('Grayson County Metropolitan Planning Organization',         'Grayson County MPO', 'Paris'),
  ('Houston-Galveston Area Council of Government',              'HGAC MPO', 'Houston'),
  ('Killeen-Temple Metropolitan Organization',                  'Killeen-Temple MPO', 'Waco'),
  ('Laredo Metropolitan Planning Organization',                 'Laredo Webb County Area MPO', 'Laredo'),
  ('Longview Metropolitan Planning Organization',               'Longview MPO', 'Tyler'),
  ('City of Lubbock Metropolitan Planning Organization',        'Lubbock MPO', 'Lubbock'),
  ('North Central Texas Council of Governments',                'NCTCOG MPO', 'Fort Worth'),
  ('Permian Basin Metropolitan Planning Organization',          'Permian Basin MPO', 'Odessa'),
  ('Rio Grande Valley Metropolitan Planning Organization',      'Rio Grande Valley MPO', 'Pharr'),
  ('San Angelo Metropolitan Planning Organization',             'San Angelo MPO', 'San Angelo'),
  ('South East Texas Regional Planning Commission',             'SETRPC MPO', 'Beaumont'),
  ('Texarkana  Metropolitan Planning Organization',             'Texarkana MPO', 'Atlanta'),
  ('Tyler Area Metropolitan Planning Organization',             'Tyler MPO', 'Tyler'),
  ('Victoria Metropolitan Planning Organization',               'Victoria MPO', 'Yoakum'),
  ('Waco Metropolitan Planning Organization',                   'Waco MPO', 'Waco'),
  ('Wichita Falls Metropolitan Planning Organization',          'Wichita Falls MPO', 'Wichita Falls');

-- -- Add DISTRICT column
-- ALTER TABLE REF.MPO_REFERENCE
-- ADD COLUMN DISTRICT STRING;

-- -- Adjust uniqueness: now allow same MPO_DESCRIPTION in multiple districts if needed
-- ALTER TABLE REF.MPO_REFERENCE
-- DROP CONSTRAINT MPO_REFERENCE_UQ;

-- ALTER TABLE REF.MPO_REFERENCE
-- ADD CONSTRAINT MPO_REFERENCE_UQ UNIQUE (MPO_DESCRIPTION, DISTRICT);

-- -- Populate DISTRICT for each MPO using MPO_SHORT as the key
-- MERGE INTO REF.MPO_REFERENCE AS t
-- USING (
--     SELECT * FROM VALUES
--         ('Abilene MPO',               'Abilene'),
--         ('AAMPO',                     'San Antonio'),
--         ('Amarillo MPO',              'Amarillo'),
--         ('Bryan-College Station MPO', 'Bryan'),
--         ('CAMPO MPO',                 'Austin'),
--         ('Corpus Christi MPO',        'Corpus Christi'),
--         ('Eagle Pass MPO',            'Laredo'),
--         ('El Paso MPO',               'El Paso'),
--         ('Grayson County MPO',        'Paris'),
--         ('HGAC MPO',                  'Houston'),
--         ('Killeen-Temple MPO',        'Waco'),
--         ('Laredo Webb County Area MPO','Laredo'),
--         ('Longview MPO',              'Tyler'),
--         ('Lubbock MPO',               'Lubbock'),
--         ('NCTCOG MPO',                'Fort Worth'),
--         ('Permian Basin MPO',         'Odessa'),
--         ('Rio Grande Valley MPO',     'Pharr'),
--         ('San Angelo MPO',            'San Angelo'),
--         ('SETRPC MPO',                'Beaumont'),
--         ('Texarkana MPO',             'Atlanta'),
--         ('Tyler MPO',                 'Tyler'),
--         ('Victoria MPO',              'Yoakum'),
--         ('Waco MPO',                  'Waco'),
--         ('Wichita Falls MPO',         'Wichita Falls')
-- ) AS s(MPO_SHORT, DISTRICT)
-- ON t.MPO_SHORT = s.MPO_SHORT
-- WHEN MATCHED THEN
--     UPDATE SET t.DISTRICT = s.DISTRICT;

-- Add specific duplicate entries for MPOs that serve multiple districts
INSERT INTO REF.MPO_REFERENCE (MPO_DESCRIPTION, MPO_SHORT, DISTRICT)
VALUES
  ('North Central Texas Council of Governments', 'NCTCOG MPO', 'Dallas'),
  ('North Central Texas Council of Governments', 'NCTCOG MPO', 'Paris'),
  ('Houston-Galveston Area Council of Government', 'HGAC MPO', 'Beaumont');

ALTER TABLE REF.MPO_REFERENCE ADD (COLUMN DISTRICT_ABBR STRING, COLUMN CHANGE_ORDERS_MPO STRING);
SELECT * FROM REF.MPO_REFERENCE;

UPDATE REF.MPO_REFERENCE
SET DISTRICT_ABBR =
    CASE
        WHEN MPO_SHORT = 'Abilene MPO' THEN 'ABL'
        WHEN MPO_SHORT = 'AAMPO' THEN 'SAT'
        WHEN MPO_SHORT = 'Amarillo MPO' THEN 'AMA'
        WHEN MPO_SHORT = 'Bryan-College Station MPO' THEN 'BRY'
        WHEN MPO_SHORT = 'CAMPO MPO' THEN 'AUS'
        WHEN MPO_SHORT = 'Corpus Christi MPO' THEN 'CRP'
        WHEN MPO_SHORT = 'Eagle Pass MPO' THEN 'LRD'
        WHEN MPO_SHORT = 'El Paso MPO' THEN 'ELP'
        WHEN MPO_SHORT = 'Grayson County MPO' THEN 'PAR'
        WHEN MPO_SHORT = 'HGAC MPO' THEN 'HOU/BMT'
        WHEN MPO_SHORT = 'Killeen-Temple MPO' THEN 'WAC'
        WHEN MPO_SHORT = 'Laredo Webb County Area MPO' THEN 'LRD'
        WHEN MPO_SHORT = 'Longview MPO' THEN 'TYL'
        WHEN MPO_SHORT = 'Lubbock MPO' THEN 'LBB'
        WHEN MPO_SHORT = 'NCTCOG MPO' THEN 'DAL/FTW/PAR'
        WHEN MPO_SHORT = 'Permian Basin MPO' THEN 'ODA'
        WHEN MPO_SHORT = 'Rio Grande Valley MPO' THEN 'PHR'
        WHEN MPO_SHORT = 'San Angelo MPO' THEN 'SJT'
        WHEN MPO_SHORT = 'SETRPC MPO' THEN 'BMT'
        WHEN MPO_SHORT = 'Texarkana MPO' THEN 'ATL'
        WHEN MPO_SHORT = 'Tyler MPO' THEN 'TYL'
        WHEN MPO_SHORT = 'Victoria MPO' THEN 'YKM'
        WHEN MPO_SHORT = 'Waco MPO' THEN 'WAC'
        WHEN MPO_SHORT = 'Wichita Falls MPO' THEN 'WFS'
        ELSE ''
    END;

UPDATE REF.MPO_REFERENCE
SET CHANGE_ORDERS_MPO =
    CASE
        WHEN MPO_SHORT = 'Abilene MPO' THEN ''
        WHEN MPO_SHORT = 'AAMPO' THEN '[SAT] San Antonio-Bexar Cnty TMA'
        WHEN MPO_SHORT = 'Amarillo MPO' THEN ''
        WHEN MPO_SHORT = 'Bryan-College Station MPO' THEN '[BRY] Bryan-College Station MPO'
        WHEN MPO_SHORT = 'CAMPO MPO' THEN '[AUS] CAMPO TMA'
        WHEN MPO_SHORT = 'Corpus Christi MPO' THEN ''
        WHEN MPO_SHORT = 'Eagle Pass MPO' THEN ''
        WHEN MPO_SHORT = 'El Paso MPO' THEN '[ELP] El Paso TMA'
        WHEN MPO_SHORT = 'Grayson County MPO' THEN ''
        WHEN MPO_SHORT = 'HGAC MPO' THEN '[BMT & HOU] HGAC TMA'
        WHEN MPO_SHORT = 'Killeen-Temple MPO' THEN '[WAC] Killeen-Temple MPO'
        WHEN MPO_SHORT = 'Laredo Webb County Area MPO' THEN ''
        WHEN MPO_SHORT = 'Longview MPO' THEN ''
        WHEN MPO_SHORT = 'Lubbock MPO' THEN ''
        WHEN MPO_SHORT = 'NCTCOG MPO' THEN '[DAL,FTW,PAR] NCTCOG TMA'
        WHEN MPO_SHORT = 'Permian Basin MPO' THEN ''
        WHEN MPO_SHORT = 'Rio Grande Valley MPO' THEN ''
        WHEN MPO_SHORT = 'San Angelo MPO' THEN '[SJT] San Angelo MPO'
        WHEN MPO_SHORT = 'SETRPC MPO' THEN ''
        WHEN MPO_SHORT = 'Texarkana MPO' THEN ''
        WHEN MPO_SHORT = 'Tyler MPO' THEN '[TYL] Tyler MPO'
        WHEN MPO_SHORT = 'Victoria MPO' THEN '[YKM] Victoria MPO'
        WHEN MPO_SHORT = 'Waco MPO' THEN ''
        WHEN MPO_SHORT = 'Wichita Falls MPO' THEN ''
        ELSE ''
    END;

-- 1 Case left to handle: [BMT] JHORTS MPO