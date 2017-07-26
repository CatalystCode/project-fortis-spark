package com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra

import java.time.Instant.now
import java.util.Date

import com.microsoft.partnercatalyst.fortis.spark.dto.FortisEvent
import com.microsoft.partnercatalyst.fortis.spark.transforms.gender.GenderDetector.{Female, Male}
import com.microsoft.partnercatalyst.fortis.spark.transforms.sentiment.SentimentDetector.Neutral

/*
* Created By @c-w on 6/29/2017
* Refactoring over schema class from older cassandra-sink branch
*/

case class Sentiment(
                      pos_avg: Float,
                      neg_avg: Float) extends Serializable

case class Gender(
                   male_mentions: Long,
                   female_mentions: Long) extends Serializable

case class Entities(
                     name: String,
                     externalsource: String,
                     externalrefid: String,
                     count: Long) extends Serializable

case class Place(
                  placeid: String,
                  centroidlat: Double,
                  centroidlon: Double) extends Serializable

case class Features(
                     mentions: Long,
                     sentiment: Sentiment,
                     gender: Gender,
                     keywords: Seq[String],
                     places: Seq[Place],
                     entities: Seq[Entities]) extends Serializable

case class Event(
                  pipelinekey: String,
                  computedfeatures: Features,
                  eventtime: Long,
                  eventlangcode: String,
                  eventid: String,
                  insertiontime: Long,
                  body: String,
                  batchid: String,
                  externalsourceid: String,
                  sourceurl: String,
                  title: String) extends Serializable

case class EventBatchEntry(
                            eventid: String,
                            pipelinekey: String,
                            computedfeatures: Features,
                            eventtime: Long,
                            externalsourceid: String) extends Serializable

case class EventTags(
                      pipelinekey: String,
                      eventtime: Long,
                      eventid: String,
                      centroidlat: Double,
                      centroidlon: Double,
                      externalsourceid: String,
                      topic: String,
                      placeid: String) extends Serializable

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

object CassandraEventSchema {
  def apply(item: FortisEvent, batchid: String): Event = {
    Event(
      pipelinekey = item.details.pipelinekey,
      externalsourceid = item.details.externalsourceid,
      computedfeatures = Utils.getFeature(item),
      eventtime = item.details.eventtime,
      batchid = batchid,
      eventlangcode = item.analysis.language.orNull,
      eventid = item.details.eventid,
      insertiontime = new Date().getTime,
      body = item.details.body,
      sourceurl = item.details.sourceurl,
      title = item.details.title)
  }
}

object

object CassandraEventTagsSchema {
  def apply(item: EventBatchEntry): Seq[EventTags] = {
    val res = for {
      kw <- item.computedfeatures.keywords
      location <- item.computedfeatures.places
    } yield EventTags(
      pipelinekey = item.pipelinekey,
      centroidlat = location.centroidlat,
      centroidlon = location.centroidlon,
      eventid = item.eventid,
      topic = kw.toLowerCase,
      eventtime = item.eventtime,
      externalsourceid = item.externalsourceid,
      placeid = location.placeid
    )

    println(res)
    res
  }
}

object Utils {
  def mean(items: List[Double]): Option[Double] = {
    if (items.isEmpty) {
      return None
    }

    Some(items.sum / items.length)
  }

  def getFeature(item: FortisEvent): Features = {
    val genderCounts = item.analysis.genders.map(_.name).groupBy(identity).mapValues(_.size.toLong)
    val entityCounts = item.analysis.entities.map(_.name).groupBy(identity).mapValues(_.size.toLong)
    val positiveSentiments = item.analysis.sentiments.filter(_ > Neutral)
    val negativeSentiments = item.analysis.sentiments.filter(_ < Neutral)
    Features(
      mentions = -1,
      places = item.analysis.locations.map(place => Place(placeid = place.wofId, centroidlat = place.latitude.getOrElse(-1), centroidlon = place.longitude.getOrElse(-1))),
      keywords = item.analysis.keywords.map(_.name),
      sentiment = Sentiment(
        pos_avg = rescale(positiveSentiments, 0, 1).flatMap(mean).map(_.toFloat).getOrElse(-1),
        neg_avg = rescale(negativeSentiments, 0, 1).flatMap(mean).map(_.toFloat).getOrElse(-1)),
      gender = Gender(
        male_mentions = genderCounts.getOrElse(Male, 0L),
        female_mentions = genderCounts.getOrElse(Female, 0L)),
      entities = entityCounts.map(kv => Entities(
        name = kv._1,
        count = kv._2,
        externalsource = "", // todo
        externalrefid = "" // todo
      )).toList)
  }

  /** @see https://stats.stackexchange.com/a/25897 */
  def rescale(items: List[Double], min_new: Double, max_new: Double): Option[List[Double]] = {
    if (items.isEmpty) {
      return None
    }

    val min_old = items.min
    val max_old = items.max
    if (max_old == min_old) {
      return None
    }

    val coef = (max_new - min_new) / (max_old - min_old)
    Some(items.map(v => coef * (v - max_old) + max_new))
  }
}