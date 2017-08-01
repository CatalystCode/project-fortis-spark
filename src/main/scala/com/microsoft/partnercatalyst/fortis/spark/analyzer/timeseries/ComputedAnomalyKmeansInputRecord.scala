package com.microsoft.partnercatalyst.fortis.spark.analyzer.timeseries

import com.microsoft.partnercatalyst.fortis.spark.dto.TopicCount
import org.apache.spark.ml.linalg.{Vector, Vectors}

case class ComputedAnomalyKmeansInputRecord(
  period: String,
  periodtype: String,
  pipelinekey: String,
  externalsourceid: String,
  features: Vector
) extends Serializable

object ComputedAnomalyKmeansInputRecord {

  def apply(topicCount: TopicCount,
            parameters: ComputedAnomalyKmeansModelParameter): ComputedAnomalyKmeansInputRecord = ComputedAnomalyKmeansInputRecord(
    topicCount.period,
    topicCount.periodtype,
    topicCount.pipelinekey,
    topicCount.externalsourceid,
    Vectors.dense(
      parameters.periodFeature(topicCount),
      topicCount.pipelinekey.hashCode.toDouble,
      topicCount.externalsourceid.hashCode.toDouble,
      topicCount.mentioncount.toDouble)
  )

}