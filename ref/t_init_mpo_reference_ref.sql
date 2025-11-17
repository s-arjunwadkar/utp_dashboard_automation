USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE SCHEMA IF NOT EXISTS REF;

CREATE OR REPLACE TABLE REF.MPO_REFERENCE (
  MPO_DESCRIPTION STRING NOT NULL,
  MPO_SHORT       STRING NOT NULL,
  CONSTRAINT MPO_REFERENCE_UQ UNIQUE (MPO_DESCRIPTION)  -- logical uniqueness
)
COMMENT = 'This table stores mpo names, their long and short versions. Used as reference to standardize names.';

-- Seed rows (idempotent-ish; will error on exact-duplicate MPO_DESCRIPTION because of the UNIQUE)
INSERT INTO REF.MPO_REFERENCE (MPO_DESCRIPTION, MPO_SHORT)
SELECT * FROM VALUES
  ('Abilene Metropolitan Planning Organization',                'Abilene MPO'),
  ('Alamo Area Metropolitan Planning Organization',             'AAMPO'),
  ('Amarillo Metropolitan Planning Organization',               'Amarillo MPO'),
  ('Bryan-College Station Metropolitan Planning Organization',  'Bryan-College Station MPO'),
  ('Capital Area Metropolitan Planning Organization',           'CAMPO MPO'),
  ('Corpus Christi Metropolitan Planning Organization',         'Corpus Christi MPO'),
  ('Eagle Pass Metropolitan Planning Organization',             'Eagle Pass MPO'),
  ('El Paso Metropolitan Planning Organization',                'El Paso MPO'),
  ('Grayson County Metropolitan Planning Organization',         'Grayson County MPO'),
  ('Houston-Galveston Area Council of Government',              'HGAC MPO'),
  ('Killeen-Temple Metropolitan Organization',                  'Killeen-Temple MPO'),
  ('Laredo Metropolitan Planning Organization',                 'Laredo Webb County Area MPO'),
  ('Longview Metropolitan Planning Organization',               'Longview MPO'),
  ('City of Lubbock Metropolitan Planning Organization',        'Lubbock MPO'),
  ('North Central Texas Council of Governments',                'NCTCOG MPO'),
  ('Permian Basin Metropolitan Planning Organization',          'Permian Basin MPO'),
  ('Rio Grande Valley Metropolitan Planning Organization',      'Rio Grande Valley MPO'),
  ('San Angelo Metropolitan Planning Organization',             'San Angelo MPO'),
  ('South East Texas Regional Planning Commission',             'SETRPC MPO'),
  ('Texarkana  Metropolitan Planning Organization',             'Texarkana MPO'),
  ('Tyler Area Metropolitan Planning Organization',             'Tyler MPO'),
  ('Victoria Metropolitan Planning Organization',               'Victoria MPO'),
  ('Waco Metropolitan Planning Organization',                   'Waco MPO'),
  ('Wichita Falls Metropolitan Planning Organization',          'Wichita Falls MPO');
