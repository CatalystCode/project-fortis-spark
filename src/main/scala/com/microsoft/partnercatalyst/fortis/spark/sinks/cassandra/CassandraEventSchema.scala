package com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra

import java.util.Date

import com.microsoft.partnercatalyst.fortis.spark.dto.FortisEvent
import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.dto._
import java.text.Collator
import java.util.Locale

import com.microsoft.partnercatalyst.fortis.spark.analyzer.timeseries.{Period, PeriodType}
import com.microsoft.partnercatalyst.fortis.spark.transforms.gender.GenderDetector.{Female, Male}

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

object CassandraPopularPlaces {
  def apply(item: EventBatchEntry): Seq[PopularPlaceAggregate] = {
    for {
      kw <- Utils.getConjunctiveTopics(Option(item.computedfeatures.keywords))
      location <- item.computedfeatures.places
      periodType <- Utils.getCassandraPeriodTypes
    } yield PopularPlaceAggregate(
      pipelinekey = item.pipelinekey,
      centroidlat = location.centroidlat,
      centroidlon = location.centroidlon,
      placeid = location.placeid,
      periodstartdate = Period(item.eventtime.getTime, periodType).startTime(),
      periodenddate = Period(item.eventtime.getTime, periodType).endTime(),
      periodtype = periodType.periodTypeName,
      period = periodType.format(item.eventtime.getTime),
      externalsourceid = item.externalsourceid,
      mentioncount = item.computedfeatures.mentions,
      conjunctiontopic1 = kw._1,
      conjunctiontopic2 = kw._2,
      conjunctiontopic3 = kw._3,
      avgsentiment = item.computedfeatures.sentiment.neg_avg
    )
  }
}

object CassandraEventTagsSchema {
  def apply(item: EventBatchEntry): Seq[EventTags] = {
    for {
      kw <- item.computedfeatures.keywords
      location <- item.computedfeatures.places
    } yield EventTags(
      pipelinekey = item.pipelinekey,
      centroidlat = location.centroidlat,
      centroidlon = location.centroidlon,
      eventid = item.eventid,
      topic = kw.toLowerCase,
      eventtime = item.eventtime.getTime,
      externalsourceid = item.externalsourceid,
      placeid = location.placeid
    )
  }
}

object Utils {
  private val ConjunctiveTopicComboSize = 3
  private val DefaultPrimaryLanguage = "en"//TODO thread the site settings primary language to getConjunctiveTopics

  def getCassandraPeriodTypes: Seq[PeriodType] = {
    Seq(PeriodType.Day, PeriodType.Hour, PeriodType.Month, PeriodType.Week, PeriodType.Year)
  }

  def getConjunctiveTopics(topicSeq: Option[Seq[String]], langcode: Option[String] = None): Seq[(String, String, String)] = {
    topicSeq match {
      case Some(topics) =>
        (topics ++ Seq("", "")).toList.combinations(ConjunctiveTopicComboSize).toList.map(combo => {
          val sortedCombo = combo.sortWith{(a, b) =>
            Ordering.comparatorToOrdering(Collator.getInstance(new Locale(langcode.getOrElse(langcode.getOrElse(DefaultPrimaryLanguage))))).compare(a,b) < 0 && a != ""
          }

          (sortedCombo(0), sortedCombo(1), sortedCombo(2))
        })
      case None => Seq()
    }
  }

  def getSentimentScore(sentiments: List[Double]): Float = {
    Option(sentiments) match {
      case None => 0F
      case Some(sentimentList) => {
        var neg_sent = 0F
        if(!sentimentList.isEmpty){
          neg_sent = sentimentList.head.toFloat
        }

        neg_sent
      }
    }
  }

  def getFeature(item: FortisEvent): Features = {
    val genderCounts = item.analysis.genders.map(_.name).groupBy(identity).mapValues(t=>t.size.toLong)
    val entityCounts = item.analysis.entities.map(_.name).groupBy(identity).mapValues(t=>t.size.toLong)
    val zero = 0.toLong
    Features(
      mentions = 1,
      places = item.analysis.locations.map(place => Place(placeid = place.wofId, centroidlat = place.latitude.getOrElse(-1), centroidlon = place.longitude.getOrElse(-1))),
      keywords = item.analysis.keywords.map(_.name),
      sentiment = Sentiment(neg_avg = getSentimentScore(item.analysis.sentiments)),//rescale(negativeSentiments, 0, 1).flatMap(mean).map(_.toFloat).getOrElse(1)),
      gender = Gender(
        male_mentions = genderCounts.getOrElse(Male, 0),
        female_mentions = genderCounts.getOrElse(Female, 0)),
      entities = entityCounts.map(kv => Entities(
        name = kv._1,
        count = kv._2,
        externalsource = "", // todo
        externalrefid = "" // todo
      )).toList)
  }
}