package com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.udfs

import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.dto.HeatmapEntry
import net.liftweb.json.{parse}
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.{JValue, RenderSettings, render}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

object FortisUdfFunctions {
  private val DoubleToLongConversionFactor = 1000

  val MeanAverage: (Double, Long) => Long = (aggregationMean: Double, aggregationCount: Long) => {
    ((getDouble(aggregationMean) * getLong(aggregationCount, Option(0L))) * DoubleToLongConversionFactor).toLong
  }

  val MergeHeatMap: (Seq[GenericRowWithSchema], String) => String = (aggregatedHeatMap: Seq[GenericRowWithSchema], originalHeatmap: String) => {
    val tileIdField = "detailtileid"
    val avgSentimentField = "avgsentimentagg"
    val mentionCountField = "mentioncountagg"

    def getSentimentAvg(heatmapEntry: Option[HeatmapEntry]) = heatmapEntry match {
      case None => 0D
      case hm => hm.get.avgsentimentagg
    }

    def getMentionCount(heatmapEntry: Option[HeatmapEntry]) = heatmapEntry match {
      case None => 0L
      case hm => hm.get.mentioncountagg
    }

    implicit val formats = DefaultFormats
    val heatmap = parse(originalHeatmap).extract[Map[String, HeatmapEntry]]

    val heatmapKV = aggregatedHeatMap.map(tile => tile.getAs[String](tileIdField) ->
      HeatmapEntry(
        avgsentimentagg = tile.getAs[Double](avgSentimentField),
        mentioncountagg = tile.getAs[Long](mentionCountField)
      )).toMap

    val mergedMap = heatmap ++ heatmapKV.map{case(tileId, entry) => tileId -> HeatmapEntry(
      avgsentimentagg = MeanAverage(getSentimentAvg(heatmap.get(tileId)), getMentionCount(heatmap.get(tileId))) + MeanAverage(Option(entry.avgsentimentagg).getOrElse(0D), Option(entry.mentioncountagg).getOrElse(0L)),
      mentioncountagg = getMentionCount(heatmap.get(tileId)) + Option(entry.mentioncountagg).getOrElse(0L)
    )}

    compactRender(decompose(mergedMap))
  }

  val OptionalSummation = (longArgs1: Long, longArgs2: Long) => getLong(longArgs1, Option(0L)) + getLong(longArgs2, Option(0L))

  private def getLong(number: Long, defaultValue: Option[Long] = None): Long ={
    Option(number) match {
      case None => defaultValue.getOrElse(1)
      case Some(num) => number
    }
  }

  private def compactRender(value: JValue): String = {
    render(value, RenderSettings.compact)
  }

  private def getOptionalLong(number: Option[Long], defaultValue: Option[Long] = None): Long ={
    Option(number) match {
      case None => defaultValue.getOrElse(1)
      case Some(num) => num.get
    }
  }

  private def getDouble(number: Double, defaultValue: Option[Double] = None): Double ={
    Option(number) match {
      case None => defaultValue.getOrElse(1)
      case Some(num) => num
    }
  }
}