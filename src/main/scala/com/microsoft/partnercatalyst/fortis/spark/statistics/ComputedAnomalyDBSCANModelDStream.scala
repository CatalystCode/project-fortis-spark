package com.microsoft.partnercatalyst.fortis.spark.statistics

import java.util.Date

import com.datastax.spark.connector._
import com.microsoft.partnercatalyst.fortis.spark.analyzer.timeseries._
import com.microsoft.partnercatalyst.fortis.spark.dto.{MLModel, TopicCount, TrustedSource}
import org.apache.spark.mllib.clustering.dbscan.DBSCAN
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Duration, Minutes, StreamingContext, Time}

object ComputedAnomalyDBSCANModelDStream {

  def apply(ssc: StreamingContext): DStream[MLModel] = {
    ssc.union(Seq(
//      ComputedAnomalyDBSCANModelDStream(PeriodType.Minute, ssc),
//      ComputedAnomalyDBSCANModelDStream(PeriodType.Hour, ssc),
      ComputedAnomalyDBSCANModelDStream(PeriodType.Day, ssc)
//      ComputedAnomalyDBSCANModelDStream(PeriodType.Week, ssc),
//      ComputedAnomalyDBSCANModelDStream(PeriodType.Month, ssc),
//      ComputedAnomalyDBSCANModelDStream(PeriodType.Year, ssc)
    ))
  }

  def apply(periodType: PeriodType, ssc: StreamingContext): ComputedAnomalyDBSCANModelDStream = new ComputedAnomalyDBSCANModelDStream(periodType, ssc)

}

class ComputedAnomalyDBSCANModelDStream(periodType: PeriodType, ssc: StreamingContext) extends InputDStream[MLModel](ssc) with PeriodRetrospectiveConfig {
  var session: SparkSession = _

  override def start(): Unit = {
    session = SparkSession.builder().appName(ssc.sparkContext.appName).getOrCreate()
  }

  override def stop(): Unit = {
    if (session != null) {
      session.close()
    }
  }

  override def slideDuration: Duration = Minutes(1)

  override def compute(validTime: Time): Option[RDD[MLModel]] = {
    val spark = session
    import spark.implicits._

    val trustedSourcesTable = session.sparkContext.cassandraTable[TrustedSource]("fortis", "trustedsources").cache()
    val supportedSourcesTuples = trustedSourcesTable.map(s => (s.pipelinekey, s.externalsourceid)).collect().toSet

    val periodTypeName = periodType.periodTypeName
    val periods = this.periodType.retrospectivePeriods(new Date().getTime, this)
    val topicCountFilter = ComputedAnomalyKmeansTopicCountFilter(Some(supportedSourcesTuples))
    val topicCounts = session.sparkContext.cassandraTable[TopicCount]("fortis", "topiccounts")
      .where(
        "periodtype = ? and period in ?",
        periodTypeName, periods
      )
      .filter(topicCountFilter.filter)
      .cache()

    val uniqueSourcesCount = topicCounts.toDF().select("pipelinekey", "externalsourceid").distinct().count()
    val uniqueTopicsCount = topicCounts.toDF().select("conjunctiontopics").distinct().count()

    if (uniqueSourcesCount == 0 || uniqueTopicsCount == 0) None
    else {
      val periodType = this.periodType
      val models = ComputedAnomalyKmeansModelParameter.kmeansParameters(
        periodType,
        uniqueSourcesCount.toInt,
        uniqueTopicsCount.toInt
      ).map(parameters => {
        val inputs = topicCounts.map(row=>{
          val features = ComputedAnomalyKmeansInputRecord(row, parameters).features
          Vectors.dense(features.toArray)
        })

        val model = DBSCAN.train(inputs, 100, 3, 10)
        println(s"Trained model: ${model}")

        MLModel(
          parameters.key,
          Map(
            ("type", model.getClass.getSimpleName)
          ),
          MLModel.dataFrom(model)
        )
      })
      Some(this.ssc.sparkContext.parallelize(models))
    }
  }

  override def retrospectivePeriodCountPerType(): Map[String, Int] = Map(
    (PeriodType.Minute.periodTypeName, 4 * 60), /* 4 hours */
    (PeriodType.Hour.periodTypeName, 7 * 24), /* 7 days */
    (PeriodType.Day.periodTypeName, 2 * 31), /* 2 months */
    (PeriodType.Week.periodTypeName, 2 * 52), /* 2 years */
    (PeriodType.Month.periodTypeName, 3 * 12), /* 3 years */
    (PeriodType.Year.periodTypeName, 12) /* 12 years */
  )

}
