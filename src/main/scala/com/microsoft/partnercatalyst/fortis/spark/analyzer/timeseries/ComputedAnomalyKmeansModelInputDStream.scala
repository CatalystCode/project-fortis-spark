package com.microsoft.partnercatalyst.fortis.spark.analyzer.timeseries

import java.time.{ZoneId, ZonedDateTime}
import java.util.Date

import com.datastax.spark.connector._
import com.microsoft.partnercatalyst.fortis.spark.dto.{MLModel, TopicCount, TrustedSource}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Duration, Minutes, StreamingContext, Time}

/**
  * Given that Fortis already has a notion of Frequency (i.e. different kinds of period types), detecting signal
  * frequencies (e.g. using an FFT or other methods) isn't necessary (could be useful, but not must-have). Leveraging
  * these period types, they can be used as implicit k-means cluster anchors.
  *
  * So, in theory, the normal behavior of a given topic could be represented as a set of histograms (e.g. one such
  * histogram could have x axis values of Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, and Saturday). Such
  * histograms can be modeled with a 2D k-means cluster model.
  *
  * Because having a model for each pipeline+user combination may be impractical (both because of the number of models
  * and because of how one would query Cassandra to build these), two dimensions are included in the feature space:
  * pipelinekey and externalsourceid. The hashcode of these two strings is used as their feature values (instead of
  * using something like org.apache.spark.ml.feature.StringIndexer) because the hashcode values gives enough
  * [potential] spacing between features (as opposed to values from StringIndexer which are represented by a sequence
  * of a step of 1).
  */
object ComputedAnomalyKmeansModelInputDStream {

  def apply(periodType: PeriodType, ssc: StreamingContext): ComputedAnomalyKmeansModelInputDStream = new ComputedAnomalyKmeansModelInputDStream(periodType, ssc)

  def preferedStreamingDuration(periodType: PeriodType): Duration = {
    periodType match {
      case PeriodType.Minute => Minutes(15)
      case PeriodType.Hour => Minutes(30)
      case PeriodType.Day => Minutes(60)
      case PeriodType.Month => Minutes(4*60)
      case PeriodType.Year => Minutes(12*60)
      case _ => Minutes(60)
    }
  }

}

class ComputedAnomalyKmeansModelInputDStream(periodType: PeriodType,
                                             ssc: StreamingContext) extends InputDStream[MLModel](ssc) with PeriodRetrospectiveConfig {

  var session: SparkSession = _

  override def start(): Unit = {
    session = SparkSession.builder().appName(ssc.sparkContext.appName).getOrCreate()
  }

  override def stop(): Unit = {
    if (session != null) {
      session.close()
    }
  }

  override def compute(validTime: Time): Option[RDD[MLModel]] = {
    val spark = session
    import spark.implicits._

    val trustedSourcesTable = session.sparkContext.cassandraTable[TrustedSource]("fortis", "trustedsources").cache()
    val supportedSourcesTuples = trustedSourcesTable.map(s=>(s.pipelinekey, s.externalsourceid)).collect().toSet

    val periodTypeName = periodType.periodTypeName
    val periods = this.periodType.retrospectivePeriods(new Date().getTime, this)
    val topicCounts = session.sparkContext.cassandraTable[TopicCount]("fortis", "topiccounts")
        .where(
          "periodtype = ? and period in ?",
          periodTypeName, periods
        )
        .filter(tile=>{
          tile.conjunctiontopics._2.isEmpty &&
          tile.conjunctiontopics._3.isEmpty &&
            (tile.externalsourceid == "all" || supportedSourcesTuples.contains((tile.pipelinekey, tile.externalsourceid)))
        })
        .cache()

    val uniqueSourcesCount = topicCounts.toDF().select("pipelinekey", "externalsourceid").distinct().count()
    val uniqueTopicsCount = topicCounts.toDF().select("conjunctiontopics").distinct().count()

    if (uniqueSourcesCount == 0 || uniqueTopicsCount == 0) None
    else {
      val periodType = this.periodType
      val models = kmeansParameters(periodType, uniqueSourcesCount.toInt, uniqueTopicsCount.toInt).map(parameters => {
        val algorithm = new KMeans()
        algorithm.setK(parameters.k)
        algorithm.setMaxIter(parameters.maxIter)
        algorithm.setFeaturesCol("features")

        val inputs = topicCounts.map(topicCount => ComputedAnomalyKmeansInputRecord(
          topicCount.period,
          topicCount.periodtype,
          topicCount.pipelinekey,
          topicCount.externalsourceid,
          Vectors.dense(
            parameters.periodFeature(topicCount),
            topicCount.pipelinekey.hashCode.toDouble,
            topicCount.externalsourceid.hashCode.toDouble,
            topicCount.mentioncount.toDouble)
        )).toDF()

        val model = algorithm.fit(inputs)
        val WSSSE = model.computeCost(inputs)

        MLModel(
          parameters.key,
          Map(
            ("type", model.getClass.getSimpleName),
            ("cost", WSSSE.toString)
          ),
          MLModel.dataFrom(model)
        )
      })
      Some(this.ssc.sparkContext.parallelize(models))
    }
  }

  def kmeansParameters(periodType: PeriodType,
                       uniqueSourcesCount: Int,
                       uniqueTopicsCount: Int): Seq[ComputedAnomalyKmeansParameter] = {
    periodType match {
      case PeriodType.Minute => Seq(
        ComputedAnomalyKmeansParameter(
          ComputedAnomalyKmeansModelKey.Minute,
          60 * uniqueSourcesCount * uniqueTopicsCount,
          20,
          topicCount=>{
            ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getMinute
          })
      )
      case PeriodType.Hour => Seq(
        ComputedAnomalyKmeansParameter(
          ComputedAnomalyKmeansModelKey.Hour,
          24 * uniqueSourcesCount * uniqueTopicsCount,
          20,
          topicCount=>{
            ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getHour
          })
      )
      case PeriodType.Day => Seq(
        ComputedAnomalyKmeansParameter(
          ComputedAnomalyKmeansModelKey.DayOfWeek,
          7 * uniqueSourcesCount * uniqueTopicsCount,
          20,
          topicCount=>{
            ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getDayOfWeek.getValue
          }),
        ComputedAnomalyKmeansParameter(
          ComputedAnomalyKmeansModelKey.DayOfMonth,
          31 * uniqueSourcesCount * uniqueTopicsCount, /* 31 max days in a month */
          20,
          topicCount=>{
            ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getDayOfMonth
          }),
        ComputedAnomalyKmeansParameter(
          ComputedAnomalyKmeansModelKey.DayOfYear,
          365 * uniqueSourcesCount * uniqueTopicsCount,
          20,
          topicCount=>{
            ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getDayOfYear
          })
      )
      case PeriodType.Week => Seq(
        ComputedAnomalyKmeansParameter(
          ComputedAnomalyKmeansModelKey.WeekOfMonth,
          4 * uniqueSourcesCount * uniqueTopicsCount,
          20,
          topicCount=>{
            ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getDayOfMonth/7
          }),
        ComputedAnomalyKmeansParameter(
          ComputedAnomalyKmeansModelKey.WeekOfYear,
          52 * uniqueSourcesCount * uniqueTopicsCount,
          20,
          topicCount=>{
            ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getDayOfYear/52
          })
      )
      case PeriodType.Month => Seq(
        ComputedAnomalyKmeansParameter(
          ComputedAnomalyKmeansModelKey.Month,
          12 * uniqueSourcesCount * uniqueTopicsCount,
          20,
          topicCount=>{
            ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getMonthValue
          })
      )
      case PeriodType.Year => Seq()
    }
  }

  override def retrospectivePeriodCountPerType(): Map[String, Int] = Map(
    (PeriodType.Minute.periodTypeName, 4*60), /* 4 hours */
    (PeriodType.Hour.periodTypeName, 7*24), /* 7 days */
    (PeriodType.Day.periodTypeName, 365), /* 1 year */
    (PeriodType.Week.periodTypeName, 2*52), /* 2 years */
    (PeriodType.Month.periodTypeName, 3*12), /* 3 years */
    (PeriodType.Year.periodTypeName, 7) /* 7 years */
  )

}

case class ComputedAnomalyKmeansInputRecord(
  period: String,
  periodtype: String,
  pipelinekey: String,
  externalsourceid: String,
  features: Vector
) extends Serializable

case class ComputedAnomalyKmeansParameter(
  key:String,
  k: Int,
  maxIter: Int,
  periodFeature: TopicCount=>Double
) extends Serializable
