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
