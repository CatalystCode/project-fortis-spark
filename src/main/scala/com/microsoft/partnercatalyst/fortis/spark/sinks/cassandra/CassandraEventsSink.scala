package com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra

import java.util.UUID

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf
import com.microsoft.partnercatalyst.fortis.spark.dba.ConfigurationManager
import com.microsoft.partnercatalyst.fortis.spark.dto.FortisEvent
import com.microsoft.partnercatalyst.fortis.spark.logging.Loggable
import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.aggregators._
import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.dto._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

import scala.util.{Failure, Success, Try}

object CassandraEventsSink extends Loggable {
  private val KeyspaceName = "fortis"
  private val TableEvent = "events"
  private val TableEventPlaces = "eventplaces"
  private val TableEventBatches = "eventbatches"
  private val CassandraFormat = "org.apache.spark.sql.cassandra"

  def apply(dstream: DStream[FortisEvent], sparkSession: SparkSession, configurationManager: ConfigurationManager): Unit = {
    implicit lazy val connector: CassandraConnector = CassandraConnector(sparkSession.sparkContext)

    dstream
    .map(event => event.copy(analysis = event.analysis.copy(
      keywords = event.analysis.keywords.distinct,
      locations = event.analysis.locations.distinct,
      entities = event.analysis.entities.distinct
    )))
    .foreachRDD { eventsRDD => {
      eventsRDD.cache()

      if (!eventsRDD.isEmpty) {
        val batchSize = eventsRDD.count()
        val batchid = UUID.randomUUID().toString
        val fortisEventsRDD = eventsRDD.map(CassandraEventSchema(_, batchid))

        val fortisEventsRDDRepartitioned = fortisEventsRDD.repartition(2 * sparkSession.sparkContext.defaultParallelism).cache()

        writeFortisEvents(fortisEventsRDDRepartitioned)

        val offlineAggregators = Seq[OfflineAggregator[_]](
          new ConjunctiveTopicsOffineAggregator(configurationManager),
          new PopularPlacesOfflineAggregator(configurationManager),
          new HeatmapOfflineAggregator(configurationManager)
        )

        val filteredEvents = removeDuplicates(batchid, fortisEventsRDDRepartitioned)
        filteredEvents.cache()

        writeEventBatchToEventTagTables(filteredEvents, sparkSession.sparkContext)

        val eventsExploded = filteredEvents.flatMap(event=>{
          Seq(
            event,
            event.copy(externalsourceid = "all"),
            event.copy(pipelinekey = "all", externalsourceid = "all")
          )
        })

        offlineAggregators.foreach(aggregator => {
          val aggregatorName = aggregator.getClass.getSimpleName
          Try(aggregator.aggregateAndSave(eventsExploded, KeyspaceName)) match {
            case Failure(ex) =>
              logError(s"Failed performing offline aggregation $aggregatorName", ex)
              logDependency("pipeline.sink", s"cassandra.$aggregatorName", success = false)

            case Success(_) =>
              logDependency("pipeline.sink", s"cassandra.$aggregatorName", success = true)
          }
        })

        eventsExploded.unpersist(blocking = true)
        filteredEvents.unpersist(blocking = true)
        fortisEventsRDDRepartitioned.unpersist(blocking = true)
        fortisEventsRDD.unpersist(blocking = true)
        eventsRDD.unpersist(blocking = true)
      }
    }}

    def writeFortisEvents(events: RDD[Event]): Unit = {
      Try(events.saveToCassandra(KeyspaceName, TableEvent, writeConf = WriteConf(ifNotExists = true))) match {
        case Failure(ex) =>
          logError(s"Failed writing to $TableEvent", ex)
          logDependency("pipeline.sink", s"cassandra.$TableEvent", success = false)

        case Success(_) =>
          logDependency("pipeline.sink", s"cassandra.$TableEvent", success = true)
      }
    }

    def removeDuplicates(batchid: String, events: RDD[Event]): RDD[Event] = {
      events.repartitionByCassandraReplica(KeyspaceName, TableEventBatches)
      val filteredEvents = events.joinWithCassandraTable(
        KeyspaceName,
        TableEventBatches,
        selectedColumns = SomeColumns("eventid", "batchid"),
        joinColumns = SomeColumns("eventid", "batchid")
      ).map(_._1)

      filteredEvents
    }

    def writeEventBatchToEventTagTables(events: RDD[Event], sparkContext: SparkContext): Unit = {
      val defaultZoom = configurationManager.fetchSiteSettings(sparkContext).defaultzoom

      Try(events.flatMap(CassandraEventPlacesSchema(_, defaultZoom)).saveToCassandra(KeyspaceName, TableEventPlaces)) match {
        case Failure(ex) =>
          logError(s"Failed writing to $TableEventPlaces", ex)
          logDependency("pipeline.sink", s"cassandra.$TableEventPlaces", success = false)

        case Success(_) =>
          logDependency("pipeline.sink", s"cassandra.$TableEventPlaces", success = true)
      }
    }
  }
}