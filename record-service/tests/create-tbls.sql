-- Create databases
CREATE DATABASE IF NOT EXISTS tpch;
CREATE DATABASE IF NOT EXISTS rs;

-- Create tables
DROP TABLE IF EXISTS tpch.nation;
CREATE EXTERNAL TABLE tpch.nation (
  N_NATIONKEY SMALLINT,
  N_NAME STRING,
  N_REGIONKEY SMALLINT,
  N_COMMENT STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/test-warehouse/tpch.nation';

-- TODO: really make this all types.
DROP TABLE IF EXISTS rs.alltypes;
CREATE TABLE rs.alltypes(
  bool_col BOOLEAN,
  tinyint_col TINYINT,
  smallint_col SMALLINT,
  int_col INT,
  bigint_col BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  string_col STRING,
  varchar_col VARCHAR(10),
  char_col CHAR(5),
  timestamp_col TIMESTAMP)
STORED AS TEXTFILE;

DROP TABLE IF EXISTS rs.alltypes_null;
CREATE TABLE rs.alltypes_null like rs.alltypes;

DROP TABLE IF EXISTS rs.alltypes_empty;
CREATE TABLE rs.alltypes_empty like rs.alltypes;

-- Populate the table with two inserts, this creates two files/two blocks.
insert overwrite rs.alltypes VALUES(true, 0, 1, 2, 3, 4.0, 5.0, "hello",
  cast("vchar1" as VARCHAR(10)),
  cast("char1" as CHAR(5)),
  "2015-01-01");
insert into rs.alltypes VALUES(false, 6, 7, 8, 9, 10.0, 11.0, "world",
  cast("vchar2" as VARCHAR(10)),
  cast("char2" as CHAR(5)),
  "2016-01-01");

insert overwrite rs.alltypes_null VALUES(
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

