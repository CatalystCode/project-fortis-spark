package com.microsoft.partnercatalyst.fortis.spark.dto

import java.sql.Timestamp

case class ComputedAnomaly(
  period: String,
  periodtype: String,
  periodstartdate: Timestamp,
  pipelinekey: String,
  externalsourceid: String,
  metadata: Map[String, String],
  topic: String,
  insertiontime: Timestamp
) extends Serializable
