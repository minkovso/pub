# Наполнение DDS
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE 
  dds.incident_dds
PARTITION (
  p_date
)
SELECT DISTINCT
  CAST(SUBSTR(incident_number, 2) AS BIGINT) AS incident_id
, CAST(occurred_on_date AS TIMESTAMP) AS incident_dttm
, district 
, reporting_area 
, shooting 
, street 
, lat 
, long 
, CONCAT(year_, month_) AS p_date
FROM 
  stg.crime_stg
WHERE 
  CAST(SUBSTR(incident_number, 2) AS BIGINT) IS NOT NULL
 
INSERT INTO TABLE
  dds.crime_dds
SELECT
  CAST(SUBSTR(incident_number, 2) AS BIGINT) AS incident_id
, CAST(offense_code AS INT) AS offense_id
, ucr_part
FROM
  stg.crime_stg
WHERE
  CAST(SUBSTR(incident_number, 2) AS BIGINT) IS NOT NULL
  AND CAST(offense_code AS INT) IS NOT NULL
-------------
WITH
  q1
AS (
  SELECT
    CAST(code AS INT) AS code
  , MAX(name) AS name
  FROM
    stg.offense_stg
  WHERE
    CAST(code AS INT) IS NOT NULL
  GROUP BY
    CAST(code AS INT)
)
, q2
AS (
  SELECT
    CAST(offense_code AS INT) AS code
  , offense_code_group
  , offense_description
  , ROW_NUMBER() OVER(PARTITION BY CAST(offense_code AS INT) ORDER BY occurred_on_date DESC) AS rn
  FROM
    stg.crime_stg
  WHERE 
    CAST(offense_code AS INT) IS NOT NULL
)
, q4
AS (
  SELECT
    COALESCE(q1.code, q3.code) AS offense_id
  , q1.name
  , q3.offense_code_group AS group_name
  , q3.offense_description AS description
  FROM
    q1
  FULL JOIN
    (SELECT * FROM q2 WHERE rn = 1) q3
  ON
    q1.code = q3.code
)
INSERT OVERWRITE TABLE
  dds.offense_dds
SELECT
  COALESCE(q4.offense_id, t.offense_id) AS offense_id
, COALESCE(q4.name, t.name) AS name
, COALESCE(q4.group_name, t.group_name) AS group_name
, COALESCE(q4.description, t.description) AS description
FROM
  q4
FULL JOIN
  dds.offense_dds t
ON
  q4.offense_id = t.offense_id
# Очистка STG
TRUNCATE TABLE stg.crime_stg
TRUNCATE TABLE stg.offense_stg
