# Run w/ Text:
var path = "hdfs://localhost:20500/test-warehouse/tpch10gb.db/lineitem/*"
var path = "hdfs://localhost:20500/test-warehouse/tpch.lineitem/*"

var lineitem = sc.textFile(path)
lineitem.map(line => line.split('|')(1).toLong).reduce(_ + _)

# Run w/ Parquet:
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val data = sqlContext.parquetFile(path)

v2:
data.registerTempTable("lineitem")
sqlContext.sql("select sum(l_partkey) from lineitem").collect()


# With RS
val data = sc.recordServiceRecords("select l_partkey from tpch10gb_parquet.lineitem").
    map(v => v(0).asInstanceOf[org.apache.hadoop.io.LongWritable].get()).
    reduce(_ + _)

