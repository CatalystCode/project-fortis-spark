package com.microsoft.partnercatalyst.fortis.spark.dto

case class TopicCount(
  periodtype: String,
  period: String,
  periodstartdate: Long,
  periodenddate: Long,
  pipelinekey: String,
  externalsourceid: String,
  conjunctiontopics: (Option[String], Option[String], Option[String]),
  tilez: Int,
  tilex: Int,
  tiley: Int,
  mentioncount: Long
) extends Serializable

object TopicCount {
  def apply(computedTile: ComputedTile): TopicCount = new TopicCount(
    computedTile.periodtype,
    computedTile.period,
    computedTile.periodstartdate,
    computedTile.periodenddate,
    computedTile.pipelinekey,
    computedTile.externalsourceid,
    computedTile.conjunctiontopics,
    computedTile.tilez,
    computedTile.tilex,
    computedTile.tiley,
    computedTile.mentioncount
  )
}