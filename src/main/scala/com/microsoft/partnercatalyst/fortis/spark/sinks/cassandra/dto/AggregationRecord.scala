package com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.dto

trait AggregationRecord {
  val periodstartdate: Long
  val periodenddate: Long
  val periodtype: String
  val period: String
  val pipelinekey: String
  val mentioncount: Long
  val avgsentiment: Float
  val externalsourceid: String
}