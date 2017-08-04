package com.microsoft.partnercatalyst.fortis.spark.statistics

import java.sql.Timestamp

import com.datastax.spark.connector._
import com.microsoft.partnercatalyst.fortis.spark.analyzer.timeseries.PeriodType
import com.microsoft.partnercatalyst.fortis.spark.dto.{ComputedAnomaly, ComputedTile, MLModel, TopicCount}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.joda.time.Instant

object ComputedTileToKmeansComputedAnomaly {

  def apply(tiles: RDD[ComputedTile], session: SparkSession): RDD[ComputedAnomaly] = {
    tiles.flatMap(tile => {
      val topicCount = TopicCount(tile)
      if (!ComputedAnomalyKmeansTopicCountFilter().filter(topicCount)) Seq()
      else models(tile, session).flatMap(model => {
        val parameters = ComputedAnomalyKmeansModelParameter.parametersForAnalysisByKey(model.key)
        val input = ComputedAnomalyKmeansInputRecord(topicCount, parameters)
        val kmeansModel = model.dataAs[KMeansModel]()
        val tileDF = session.createDataFrame(Seq(input))

        val modelCost = model.metadata("cost").toDouble
        val computedCost = kmeansModel.computeCost(tileDF)
        if (computedCost < modelCost) Seq()
        else Seq(
          ComputedAnomaly(
            tile.period,
            tile.periodtype,
            new Timestamp(tile.periodstartdate),
            tile.pipelinekey,
            tile.externalsourceid,
            Map(("modelCost", modelCost.toString), ("computedCost", computedCost.toString)),
            tile.conjunctiontopics._1.get,
            new Timestamp(Instant.now().getMillis)
          )
        )
      })
    })
  }

  def models(tile: ComputedTile, session: SparkSession): Seq[MLModel] = {
    val keys = ComputedAnomalyKmeansModelKey.byPeriodType.get(PeriodType(tile.periodtype))
    session.sparkContext.cassandraTable[MLModel]("fortis", "mlmodel")
      .where(
        "key in ?",
        keys
      )
      .collect()
  }

}
