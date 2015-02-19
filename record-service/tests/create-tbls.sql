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
  string_col STRING)
STORED AS TEXTFILE;

-- Populate the table with two inserts, this creates two files/two blocks.
insert overwrite rs.alltypes VALUES(true, 0, 1, 2, 3, 4.0, 5.0, "hello");
insert into rs.alltypes VALUES(false, 6, 7, 8, 9, 10.0, 11.0, "world");


