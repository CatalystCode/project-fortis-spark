package com.microsoft.partnercatalyst.fortis.spark.analyzer.timeseries

import java.util.{Calendar, Date, GregorianCalendar, TimeZone}

import com.datastax.spark.connector._
import com.microsoft.partnercatalyst.fortis.spark.dto.{ComputedTile, MLModel, TopicCount, TrustedSource}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Duration, Minutes, StreamingContext, Time}

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

    val pt = this.periodType
    val k = ComputedAnomalyKmeansInputRecord.getK(pt, uniqueSourcesCount.toInt, uniqueTopicsCount.toInt)
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setMaxIter(20)
    kmeans.setFeaturesCol("features")
    kmeans.setPredictionCol("prediction")

    val inputs = topicCounts.map(tile=>ComputedAnomalyKmeansInputRecord(
      tile.period,
      periodTypeName,
      tile.pipelinekey,
      tile.externalsourceid,
      features = ComputedAnomalyKmeansInputRecord.features(pt, tile)
    )).toDF()
    val model = kmeans.fit(inputs)
    val WSSSE = model.computeCost(inputs)

    Some(this.ssc.sparkContext.parallelize(Seq(
      MLModel(
        s"kmeans.${periodTypeName}.day_of_month",
        Map(
          ("type", model.getClass.getSimpleName),
          ("cost", WSSSE.toString)
        ),
        MLModel.dataFrom(model)
      )
    )))
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

private object ComputedAnomalyKmeansInputRecord extends Serializable {

  def getK(periodType: PeriodType,
           uniqueSourcesCount: Int,
           uniqueTopicsCount: Int): Int = {
    var k = 31 /* max days in a month */
    k *= uniqueSourcesCount
    k *= uniqueTopicsCount
    k
  }

  def features(periodType: PeriodType,
               topicCount: TopicCount): Vector = {
    val calendar = new GregorianCalendar()
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"))
    calendar.setTimeInMillis(topicCount.periodstartdate)

    val periodFeature = periodType match {
      case PeriodType.Day => {
        calendar.get(Calendar.DAY_OF_MONTH)
      }
      case _ => 0.0
    }
    Vectors.dense(
      periodFeature,
      topicCount.pipelinekey.hashCode.toDouble,
      topicCount.externalsourceid.hashCode.toDouble,
      topicCount.mentioncount.toDouble)
  }

}

case class ComputedAnomalyKmeansInputRecord(period: String,
                                            periodtype: String,
                                            pipelinekey: String,
                                            externalsourceid: String,
                                            features: Vector
                                           ) extends Serializable

