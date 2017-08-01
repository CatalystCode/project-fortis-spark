package com.microsoft.partnercatalyst.fortis.spark.dto

case class TrustedSource(
  pipelinekey: String,
  externalsourceid: String,
  sourcetype: String,
  rank: Int,
  insertion_time: Long
) extends Serializable
