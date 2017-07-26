package com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra

import java.util.UUID

import com.datastax.spark.connector.writer.WriteConf
import com.microsoft.partnercatalyst.fortis.spark.dto.FortisEvent
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object CassandraEventsSink {
  private val KeyspaceName = "fortis"
  private val TableEvent = "events"
  private val TableEventTags = "eventtags"
  private val TableEventBatches = "eventbatches"
  private val CassandraFormat = "org.apache.spark.sql.cassandra"

  def apply(dstream: Option[DStream[FortisEvent]]): Unit = {
    if (dstream.isDefined) {
      dstream.get.foreachRDD(eventsRDD => {
          val session = SparkSession.builder().config(eventsRDD.sparkContext.getConf)
                        .appName(eventsRDD.sparkContext.appName)
                        .getOrCreate()
          val batchid = UUID.randomUUID().toString
          val eventDF = writeDStreamToEventsTable(eventsRDD, batchid, session)
          val eventtagsDS = writeEventsDSToEventTagsTable(eventDF, session)

      })
    }
  }

  /*Writes a Dstream of FortisEvents into the events table and fetches the newly added events based on the batch id*/
  private def writeDStreamToEventsTable(eventsRDD: RDD[FortisEvent], batchid: String, session: SparkSession): Dataset[EventBatchEntry] = {
    import session.implicits._

    eventsRDD.map(CassandraEventSchema(_, batchid)).saveToCassandra(KeyspaceName, TableEvent, writeConf = WriteConf(ifNotExists = true))
    val addedEventsDF = session.sqlContext.read.format(CassandraFormat)
           .options(Map("keyspace" -> KeyspaceName, "table" -> TableEventBatches))
           .load()

    addedEventsDF.createOrReplaceTempView(TableEventBatches)
    session.sqlContext.sql(s"select eventid, pipelinekey, eventtime, computedfeatures, externalsourceid " +
      s"                     from $TableEventBatches " +
      s"                     where batchid == '$batchid'")
            .as[EventBatchEntry]
  }

  private def aggregateEventsForPopularPlaces()

  private def writeEventsDSToEventTagsTable(eventDS: Dataset[EventBatchEntry], session: SparkSession): Dataset[EventTags] = {
    import session.sqlContext.implicits._

    val eventtagsDS = eventDS.flatMap(CassandraEventTagsSchema(_))
    eventtagsDS.write
      .format(CassandraFormat)
      .mode(SaveMode.Ignore)
      .options(Map("keyspace" -> KeyspaceName, "table" -> TableEventTags)).save

    eventtagsDS
  }
}
