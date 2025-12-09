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
