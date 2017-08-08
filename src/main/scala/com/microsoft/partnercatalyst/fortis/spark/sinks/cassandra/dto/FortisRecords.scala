package com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.dto

import java.sql.Timestamp
import scala.collection.mutable.{TreeSet}

case class Event(
                  pipelinekey: String,
                  computedfeatures: Features,
                  eventtime: Long,
                  eventlangcode: String,
                  eventid: String,
                  insertiontime: Long,
                  body: String,
                  fulltext: String,
                  batchid: String,
                  externalsourceid: String,
                  topics: Seq[String],
                  placeids: Seq[String],
                  sourceurl: String,
                  title: String) extends Serializable

case class EventBatchEntry(
                            eventid: String,
                            pipelinekey: String) extends Serializable

case class EventTopics(
                      pipelinekey: String,
                      insertiontime: Long,
                      eventids: Seq[String],
                      externalsourceid: String,
                      topic: String) extends Serializable

case class EventPlaces(
                        pipelinekey: String,
                        insertiontime: Long,
                        eventids: Seq[String],
                        centroidlat: Double,
                        centroidlon: Double,
                        conjunctiontopic1: String,
                        conjunctiontopic2: String,
                        conjunctiontopic3: String,
                        externalsourceid: String,
                        placeid: String) extends Serializable

case class PopularPlaceAggregate(
                                  override val periodstartdate: Long,
                                  override val externalsourceid: String,
                                  override val periodenddate: Long,
                                  override val periodtype: String,
                                  override val period: String,
                                  override val pipelinekey: String,
                                  override val mentioncount: Long,
                                  override val avgsentimentnumerator: Long,
                                  override val avgsentiment: Float,
                                  placeid: String,
                                  centroidlat: Double,
                                  centroidlon: Double,
                                  conjunctiontopic1: String,
                                  conjunctiontopic2: String,
                                  conjunctiontopic3: String
                        ) extends AggregationRecord with Serializable

case class PopularTopicAggregate(
                                  override val periodstartdate: Long,
                                  override val externalsourceid: String,
                                  override val periodenddate: Long,
                                  override val periodtype: String,
                                  override val period: String,
                                  override val pipelinekey: String,
                                  override val mentioncount: Long,
                                  override val avgsentimentnumerator: Long,
                                  override val avgsentiment: Float,
                                  override val tilex: Int,
                                  override val tilez: Int,
                                  override val tiley: Int,
                                  topic: String
                                ) extends AggregationRecordTile with Serializable

case class SiteSetting(
                        id: String,
                        sitename: String,
                        geofence: Seq[Double],
                        languages: Set[String],
                        defaultzoom: Int,
                        title: String,
                        logo: String,
                        translationsvctoken: String,
                        cogspeechsvctoken: String,
                        cogvisionsvctoken: String,
                        cogtextsvctoken: String,
                        insertion_time: Long
                      )

case class Stream(
                   pipeline: String,
                   streamid: Long,
                   connector: String,
                   params: Map[String, String])

case class TrustedSource(
                          sourceid: String,
                          sourcetype: String,
                          connector: String,
                          rank: Int,
                          insertion_time: Long)
