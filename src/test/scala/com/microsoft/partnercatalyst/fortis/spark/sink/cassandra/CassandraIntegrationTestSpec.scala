package com.microsoft.partnercatalyst.fortis.spark.sink.cassandra

import java.sql.Timestamp
import java.util.Date

import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra._
import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.dto._
import org.joda.time.DateTime
import org.scalatest.FlatSpec

class CassandraIntegrationTestSpec extends FlatSpec {
  it should "verify that we can produce conjunctive topic tuples from a list of topics" in {
    val conjunctiveTopics = Utils.getConjunctiveTopics(Option(Seq("sam", "erik", "tom")))
    val expectedHeadItem = (Some("erik"), Some("sam"), Some("tom"))
    assert(conjunctiveTopics.length === 7)
    assert(conjunctiveTopics(0) === expectedHeadItem)
  }

  it should "produce tag entry records off a fortis event" in {
    val testList = List(EventBatchEntry(eventtime = new Timestamp(new Date().getTime),
      eventid = "1122",
      pipelinekey = "twitter",
      externalsourceid = "cnn",
      computedfeatures = Features(
        mentions = -1,
        sentiment = Sentiment(neg_avg = 0),
        places = List(Place(placeid = "2134", centroidlat = 12.21, centroidlon = 43.1), Place(placeid = "213", centroidlat = 11.21, centroidlon = 43.1)),
        gender = Gender(male_mentions = 0, female_mentions = 0),
        keywords = List("isis", "car", "bomb"),
        entities = List(Entities(externalsource = "", externalrefid = "", name = "putin", count = 1)))))

    val flattenedMap = testList.flatMap(CassandraEventTagsSchema(_))
    assert(flattenedMap.length === 6)
    assert(flattenedMap(0).centroidlat > 0)
    assert(flattenedMap(0).centroidlon > 0)
    assert(!flattenedMap(0).topic.isEmpty)
  }
}
