package com.cloudera.recordservice.example

import com.cloudera.recordservice.spark.SchemaRecordServiceRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Example that uses the SchemaRecordService RDD
 */
// First declare your "row object" as a case class. This must match (with some resolution
// rules) the schema of the result from the RecordService.

// In this case we are matching by ordinal so the types of the fields must be a prefix
// of the table/query
case class NationByOrdinal(var key:Short, var name:String, var regionKey:Short)

// In this case we are matching by name so the fields must be a subset of the table/query
case class NationByName(var n_name:String)

object SchemaRDDExample {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
        .setAppName("SchemaRDD Example")
        .setMaster("local")
    val sc = new SparkContext(sparkConf)

    // Create the RDD of your type and specify where the data is. You can either
    // do this by specifying the table or sql statement.
    val dataByName = new SchemaRecordServiceRDD[NationByName](
        sc, classOf[NationByName], false).setStatement("select * from tpch.nation")
    val dataByOrdinal = new SchemaRecordServiceRDD[NationByOrdinal](
        sc, classOf[NationByOrdinal], true).setTable("tpch.nation")

    // Operate on the RDD with your row objects
    dataByOrdinal.foreach(n => println(n.key + ", " + n.name))
    dataByName.foreach(n => println(n.n_name))

    sc.stop()
  }
}
