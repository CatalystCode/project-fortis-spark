package com.microsoft.partnercatalyst.fortis.spark.statistics

import com.microsoft.partnercatalyst.fortis.spark.dto.TopicCount

case class ComputedAnomalyKmeansTopicCountFilter(supportedSourcesTuples: Option[Set[(String, String)]] = None) extends Serializable {

  def filter(tile: TopicCount): Boolean = {
    if (tile.conjunctiontopics._2.isDefined) false
    else if (tile.conjunctiontopics._3.isDefined) false
    else {
      if (supportedSourcesTuples.isEmpty) true
      else tile.externalsourceid == "all" || supportedSourcesTuples.get.contains((tile.pipelinekey, tile.externalsourceid))
    }
  }

}
