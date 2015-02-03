var tpch = sc.textFile("hdfs://localhost:20500/test-warehouse/tpch10gb.db/lineitem/*")
var q2 = tpch.map(line => line.split('|')(15)).reduce((x,y) => if (x < y) x else y)


val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.createSchemaRDD
val data = sqlContext.parquetFile("hdfs://localhost:20500/test-warehouse/tpch.nation_parquet/1543bca11941c8fa-906356d8fd9b3a95_1911128685_data.0.parq")
