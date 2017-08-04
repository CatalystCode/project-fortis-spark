package com.microsoft.partnercatalyst.fortis.spark.statistics

import java.util.Date

import com.datastax.spark.connector._
import com.microsoft.partnercatalyst.fortis.spark.analyzer.timeseries._
import com.microsoft.partnercatalyst.fortis.spark.dto.{MLModel, TopicCount, TrustedSource}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Duration, Minutes, StreamingContext, Time}

/**
  * Given that Fortis has a notion of Frequency (i.e. different kinds of period types), detecting signal frequencies
  * (e.g. using an FFT or other methods) isn't necessary (could be useful, but not must-have). Leveraging these period
  * types, they can be used as implicit k-means cluster anchors.
  *
  * So, in theory, the normal behavior of a given topic could be represented as a set of histograms (e.g. one such
  * histogram could have x axis values of Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, and Saturday). Such
  * histograms can be modeled with a 2D k-means cluster model.
  *
  * Because having a model for each pipeline+user combination may be impractical (both because of the number of models
  * and because of how one would query Cassandra to build these), two dimensions are included in the feature space:
  * pipelinekey and externalsourceid. The hashCode of these two strings is used as their feature values (instead of
  * using something like org.apache.spark.ml.feature.StringIndexer) because the hashcode values gives enough
  * [potential] spacing between features (as opposed to values from StringIndexer which are represented by a sequence
  * of a step of 1).
  */
object ComputedAnomalyKmeansModelInputDStream {

  def apply(ssc: StreamingContext): DStream[MLModel] = {
    ssc.union(Seq(
      ComputedAnomalyKmeansModelInputDStream(PeriodType.Minute, ssc),
      ComputedAnomalyKmeansModelInputDStream(PeriodType.Hour, ssc),
      ComputedAnomalyKmeansModelInputDStream(PeriodType.Day, ssc),
      ComputedAnomalyKmeansModelInputDStream(PeriodType.Week, ssc),
      ComputedAnomalyKmeansModelInputDStream(PeriodType.Month, ssc),
      ComputedAnomalyKmeansModelInputDStream(PeriodType.Year, ssc)
    ))
  }

  def apply(periodType: PeriodType, ssc: StreamingContext): ComputedAnomalyKmeansModelInputDStream = new ComputedAnomalyKmeansModelInputDStream(periodType, ssc)

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

  override def slideDuration: Duration = Minutes(15)

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
        val algorithm = new KMeans()
        algorithm.setK(parameters.k)
        algorithm.setMaxIter(parameters.maxIter)
        algorithm.setFeaturesCol("features")

        val inputs = topicCounts.map(ComputedAnomalyKmeansInputRecord(_, parameters)).toDF()
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

  override def retrospectivePeriodCountPerType(): Map[String, Int] = Map(
    (PeriodType.Minute.periodTypeName, 4 * 60), /* 4 hours */
    (PeriodType.Hour.periodTypeName, 7 * 24), /* 7 days */
    (PeriodType.Day.periodTypeName, 2 * 365), /* 2 years */
    (PeriodType.Week.periodTypeName, 2 * 52), /* 2 years */
    (PeriodType.Month.periodTypeName, 3 * 12), /* 3 years */
    (PeriodType.Year.periodTypeName, 12) /* 12 years */
  )

}

