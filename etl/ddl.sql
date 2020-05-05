# Создание директорий для архивов
hdfs dfs -mkdir /user/hive/arch
hdfs dfs -mkdir /user/hive/arch/crime_arch
hdfs dfs -mkdir /user/hive/arch/offense_codes
hdfs dfs -chmod a+w /user/hive/arch
hdfs dfs -chmod a+w /user/hive/arch/crime_arch
hdfs dfs -chmod a+w /user/hive/arch/offense_codes
# Создание схем STG, DDS, DM
CREATE SCHEMA stg
CREATE SCHEMA dds
CREATE SCHEMA dm
# Создание таблиц STG
CREATE TABLE 
  stg.crime_stg 
(
  incident_number STRING
, offense_code STRING
, offense_code_group STRING
, offense_description STRING
, district STRING
, reporting_area STRING
, shooting STRING
, occurred_on_date STRING
, year_ STRING
, month_ STRING
, day_of_week STRING
, hour_ STRING
, ucr_part STRING
, street STRING
, lat STRING
, long STRING
, location_ STRING
)
ROW FORMAT SERDE 
  "org.apache.hadoop.hive.serde2.OpenCSVSerde"
WITH SERDEPROPERTIES (
   "skip.header.line.count" = "1"
)
-------------
CREATE TABLE
  stg.offense_stg
(
  code STRING
, name STRING
)
ROW FORMAT SERDE 
  "org.apache.hadoop.hive.serde2.OpenCSVSerde"
WITH SERDEPROPERTIES (
   "skip.header.line.count" = "1"
)
# Создание таблиц DDS
CREATE TABLE 
  dds.crime_dds
(
  incident_id BIGINT
, offense_id INT
, ucr_part STRING
)
STORED AS PARQUET
-------------
CREATE TABLE 
  dds.offense_dds
(
  offense_id INT
, name STRING
, group_name STRING
, description STRING
)
STORED AS PARQUET
-------------
CREATE TABLE
  dds.incident_dds
(
  
  incident_id BIGINT
, incident_dttm TIMESTAMP
, district STRING
, reporting_area STRING
, shooting STRING
, street STRING
, lat STRING
, long STRING
)
PARTITIONED BY (
  p_date STRING
)
STORED AS PARQUET
# Создание вьюх DM
CREATE VIEW
  dm.abt
AS
SELECT
  c.incident_id
, c.offense_id
, c.ucr_part
, i.incident_dttm
, i.district
, i.reporting_area
, i.shooting
, i.street
, i.lat
, i.long
, o.name
, o.group_name
, o.description
FROM
  dds.crime_dds c
INNER JOIN
  dds.incident_dds i
ON
  c.incident_id = i.incident_id
INNER JOIN
  dds.offense_dds o
ON
  c.offense_id = o.offense_id
-------------
CREATE VIEW
  dm.street
AS
SELECT
  street
, COUNT(*) AS incident_cnt
FROM
  dds.incident_dds
GROUP BY
  street
