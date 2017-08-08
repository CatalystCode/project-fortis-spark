package com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra

import java.util.UUID

import com.datastax.spark.connector.writer.WriteConf
import com.microsoft.partnercatalyst.fortis.spark.dto.FortisEvent
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import com.datastax.spark.connector._
import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.aggregators._
import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.dto._
import org.apache.spark.rdd.RDD
import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.udfs._
import org.apache.spark.streaming.StreamingContext

object CassandraEventsSink {
  private val KeyspaceName = "fortis"
  private val TableEvent = "events"
  private val TableEventTags = "eventtags"
  private val TableEventBatches = "eventbatches"
  private val CassandraFormat = "org.apache.spark.sql.cassandra"

  def apply(dstream: Option[DStream[FortisEvent]], ssc: StreamingContext): Unit = {
    if (dstream.isDefined) {

      val aggregators = Seq(new PopularPlacesAggregator)
      val batchStream = dstream.get.foreachRDD(eventsRDD => {
        eventsRDD.foreachPartition(partition => {
          val sparkContext = eventsRDD.sparkContext
          val session = SparkSession.builder().config(sparkContext.getConf)
            .appName(sparkContext.appName)
            .getOrCreate()
          val batchid = UUID.randomUUID().toString
          registerUDFs(session)
          writeEventRddToEventsTable(eventsRDD, batchid)
          val eventBatchDF = fecthEventBatch(batchid, session)
          writeEventsDSToEventTagsTable(eventBatchDF, session)

          aggregators.foreach(aggregator => {
            aggregateEvents(eventBatchDF, session, aggregator)
          })
        })
      })
    }
  }

  private def registerUDFs(session: SparkSession): Unit ={
    session.sqlContext.udf.register("MeanAverage", FortisUdfFunctions.MeanAverage)
    session.sqlContext.udf.register("SumMentions", FortisUdfFunctions.OptionalSummation)
    session.sqlContext.udf.register("SentimentWeightedAvg", SentimentWeightedAvg)
  }

  /*Writes a Dstream of FortisEvents into the events table and fetches the newly added events based on the batch id*/
  private def writeEventRddToEventsTable(eventsRDD: RDD[FortisEvent], batchid: String): Unit = {
    eventsRDD.map(CassandraEventSchema(_, batchid)).saveToCassandra(KeyspaceName, TableEvent, writeConf = WriteConf(ifNotExists = true))
  }

  private def fecthEventBatch(batchid: String, session: SparkSession): Dataset[EventBatchEntry] = {
     import session.implicits._

     val addedEventsDF = session.sqlContext.read.format(CassandraFormat)
    .options(Map("keyspace" -> KeyspaceName, "table" -> TableEventBatches))
    .load()

    addedEventsDF.createOrReplaceTempView(TableEventBatches)

    val ds = session.sqlContext.sql(s"select eventid, pipelinekey, eventtime, computedfeatures, externalsourceid " +
    s"                     from $TableEventBatches " +
    s"                     where batchid == '$batchid'")
    ds.cache()
    ds.as[EventBatchEntry]
  }

  private def writeEventsDSToEventTagsTable(eventDS: Dataset[EventBatchEntry], session: SparkSession): Dataset[EventTags] = {
    import session.implicits._

    val eventtagsDS = eventDS.flatMap(CassandraEventTagsSchema(_))
    eventtagsDS.write
      .format(CassandraFormat)
      .mode(SaveMode.Ignore)
      .options(Map("keyspace" -> KeyspaceName, "table" -> TableEventTags)).save

    eventtagsDS
  }

  private def aggregateEvents(eventDS: Dataset[EventBatchEntry], session: SparkSession, aggregator: FortisAggregator): Unit = {
   val targetDF = aggregator.FortisTargetTableDataFrame(session)
    targetDF.createOrReplaceTempView(aggregator.FortisTargetTablename)

    val flattenedDF = aggregator.flatMap(session, eventDS)
    flattenedDF.createOrReplaceTempView(aggregator.DataFrameNameFlattenedEvents)

    val aggregatedDF = aggregator.AggregateEventBatches(session, flattenedDF)
    aggregatedDF.createOrReplaceTempView(aggregator.DataFrameNameComputed)

    val incrementallyUpdatedDF = aggregator.IncrementalUpdate(session, aggregatedDF)
    incrementallyUpdatedDF.write
      .format(CassandraFormat)
      .mode(SaveMode.Append)
      .options(Map("keyspace" -> KeyspaceName, "table" -> aggregator.FortisTargetTablename)).save
  }
}
