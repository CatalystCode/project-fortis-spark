package com.microsoft.partnercatalyst.fortis.spark.statistics

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ComputedAnomalyDemo {

  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getSimpleName
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.input.consistency.level", "LOCAL_ONE")
      .set("spark.cassandra.output.consistency.level", "LOCAL_ONE")
      .set("output.consistency.level", "LOCAL_ONE")
      .setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Minutes(1))
    val session = SparkSession.builder().appName(ssc.sparkContext.appName).getOrCreate()

    sc.setLogLevel("ERROR")

    ComputedAnomalyDBSCANModelDStream(ssc).foreachRDD(_.saveToCassandra("fortis", "mlmodels"))
//    ComputedAnomalyKmeansModelInputDStream(ssc).foreachRDD(_.saveToCassandra("fortis", "mlmodels"))

    ssc.start()
    ssc.awaitTermination()
  }

}
