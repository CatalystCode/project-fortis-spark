package com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.aggregators

import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.dto.{AggregationRecord, EventBatchEntry}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait FortisAggregator {
 protected val KeyspaceName = "fortis"
 protected val CassandraFormat = "org.apache.spark.sql.cassandra"
 protected val AggregateFunctions = "sum(mentioncount) as mentioncountagg, SentimentWeightedAvg(IF(IsNull(avgsentiment), 0, avgsentiment), IF(IsNull(mentioncount), 0, mentioncount)) as avgsentimentagg"
 protected val IncrementalUpdateMentionsUDF = "SumMentions(a.mentioncountagg, IF(IsNull(b.mentioncount), 0, b.mentioncount)) as mentioncount"
 protected val IncrementalUpdateSentimentUDF = "MeanAverage(a.avgsentimentagg, a.mentioncountagg, IF(IsNull(b.avgsentiment), 0, b.avgsentiment), IF(IsNull(b.mentioncount), 0, b.mentioncount)) as avgsentiment"

 val DataFrameNameFlattenedEvents = "flattenedEventsDF"
 val DataFrameNameComputed = "computedDF"

 val FortisTargetTablename: String

 def FortisTargetTableDataFrame(session:SparkSession): DataFrame
 def flatMap(session: SparkSession, eventDS: Dataset[EventBatchEntry]): DataFrame
 def IncrementalUpdate(session:SparkSession, aggregatedDS: DataFrame): DataFrame
 def AggregateEventBatches(session: SparkSession, flattenedEvents: DataFrame): DataFrame
}
