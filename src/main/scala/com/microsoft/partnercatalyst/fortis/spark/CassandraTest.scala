package com.microsoft.partnercatalyst.fortis.spark

<<<<<<< HEAD
  import java.util.{Date, Locale, UUID}

  import com.microsoft.partnercatalyst.fortis.spark.dto._
=======
  import java.util.UUID

  import com.microsoft.partnercatalyst.fortis.spark.dto._
  import org.apache.spark.sql.SparkSession
>>>>>>> Rebasing
  import org.apache.spark.streaming.{Seconds, StreamingContext}
  import org.apache.spark.{SparkConf, SparkContext}

  import scala.collection.mutable
<<<<<<< HEAD

  import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra._
=======
  import java.time.Instant.now

  import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra._
  import org.apache.spark.rdd.RDD
>>>>>>> Rebasing
  import org.apache.spark.streaming.dstream.DStream

  import scala.util.Properties.envOrElse

  object CassandraTest {
    case class TestFortisEvent(
                                details: Details,
                                analysis: Analysis
                              ) extends FortisEvent

    case class TestFortisDetails(
                                  eventid: String,
                                  eventtime: Long,
                                  body: String,
                                  externalsourceid: String,
                                  title: String,
                                  pipelinekey: String,
                                  sourceurl: String,
                                  sharedLocations: List[Location] = List()
                                ) extends Details
<<<<<<< HEAD
=======

>>>>>>> Rebasing
    def main(args: Array[String]): Unit = {
      val appName = this.getClass.getSimpleName
      val conf = new SparkConf()
        .setAppName(appName)
        .set("spark.cassandra.connection.host", "13.92.93.138")
        .setIfMissing("spark.cassandra.auth.username", envOrElse("FORTIS_CASSANDRA_USER", "cassandra"))
        .setIfMissing("spark.cassandra.auth.password", envOrElse("FORTIS_CASSANDRA_PASSWORD", "cassandra"))
        .set("spark.cassandra.input.consistency.level", "LOCAL_ONE")
        .set("spark.cassandra.output.consistency.level", "LOCAL_ONE")
        .set("output.consistency.level", "LOCAL_ONE")
        .setIfMissing("spark.master", "local[*]")
      val ssc = new StreamingContext(conf, Seconds(10))
      val batchid = UUID.randomUUID().toString

      val testEventsRdd = ssc.sparkContext.parallelize(Seq(TestFortisEvent(
        details = TestFortisDetails(
<<<<<<< HEAD
            eventtime = new Date().getTime,
            eventid = "1132",
=======
            eventtime = now.getEpochSecond,
            eventid = "1122",
>>>>>>> Rebasing
            sourceurl = "http://cnn.com",
            pipelinekey = "twitter",
            sharedLocations = List(),
            externalsourceid = "cnn",
            body = "test message a new change",
            title = "twitter post" ),
        analysis = Analysis(
<<<<<<< HEAD
          sentiments = List(.5),
=======
          sentiments = List(.5,.5),
>>>>>>> Rebasing
          locations = List(Location(wofId = "1234", confidence = Option(1.0), latitude = Option(12.21), longitude = Option(43.1))),
          keywords = List(Tag(name = "isis", confidence = Option(1.0)), Tag(name ="car", confidence = Option(1.0))),
          genders = List(Tag(name = "male", confidence = Option(1.0)), Tag(name ="female", confidence = Option(1.0))),
          entities = List(Tag(name = "putin", confidence = Option(1.0))),
          language = Option("en")
<<<<<<< HEAD
        )),
        TestFortisEvent(
          details = TestFortisDetails(
            eventtime = new Date().getTime,
            eventid = "112222",
            sourceurl = "http://bbc.com",
            pipelinekey = "twitter",
            sharedLocations = List(),
            externalsourceid = "bbc",
            body = "This is a another test message",
            title = "twitter post" ),
          analysis = Analysis(
            sentiments = List(.6),
            locations = List(Location(wofId = "1234", confidence = Option(1.0), latitude = Option(12.21), longitude = Option(43.1))),
            keywords = List(Tag(name = "isis", confidence = Option(1.0)), Tag(name ="car", confidence = Option(1.0)), Tag(name ="bomb", confidence = Option(1.0)), Tag(name ="fatalities", confidence = Option(1.0))),
            genders = List(Tag(name = "male", confidence = Option(1.0)), Tag(name ="female", confidence = Option(1.0))),
            entities = List(Tag(name = "putin", confidence = Option(1.0))),
            language = Option("en")
          ))))

      val dstream = ssc.queueStream(mutable.Queue(testEventsRdd)).asInstanceOf[DStream[FortisEvent]]
      CassandraEventsSink(Option(dstream), ssc)
      ssc.start()
      ssc.awaitTermination()
=======
        ))))

      val dstream = ssc.queueStream(mutable.Queue(testEventsRdd)).asInstanceOf[DStream[FortisEvent]]
      CassandraEventsSink(Option(dstream))
      ssc.start()
      ssc.awaitTermination()
      /*val testList = List(EventBatchEntry(eventtime = now.getEpochSecond,
        eventid = "1122",
        pipelinekey = "twitter",
        externalsourceid = "cnn",
        computedfeatures = Features(
          mentions = -1,
          sentiment = Sentiment(pos_avg = 0, neg_avg = 0),
          places = List(Place(placeid = "2134", centroidlat = 12.21, centroidlon = 43.1), Place(placeid = "213", centroidlat = 11.21, centroidlon = 43.1)),
          gender = Gender(male_mentions = 0, female_mentions = 0),
          keywords = List("isis", "car", "bomb"),
          entities = List(Entities(externalsource = "", externalrefid = "", name = "putin", count = 1)))))
      //testEventsRdd.saveToCassandra("fortis", "events", writeConf = Wri
      val flattenedMap = testList.flatMap(CassandraEventTagsSchema(_))
      println(flattenedMap)*/
>>>>>>> Rebasing
        /*   = "twitter",
          externalsourceid = "cnn",
          batchid = batchid,
          computedfeatures = Features(mentions = 100,
                                      sentiment = Sentiment(pos_avg = 1.5f, neg_avg = 1.0f),
                                      gender = Gender(male_mentions = 3l, female_mentions = 10l),
                                      keywords = List("isis", "car"),
                                      places = List(Place(placeid = "1212", centroidlat = 21.23, centroidlon = 56.23)),
                                      entities = List()),
          insertiontime = now.getEpochSecond,
          eventtime = now.getEpochSecond,
          eventid = "1122",
          eventlangcode = "en",
          sourceurl = "http://cnn.com",
          body = "test message a new change",
<<<<<<< HEAD
          title = "twitter post")))*/

=======
          title = "twitter post")))

        val dstream = ssc.queueStream(mutable.Queue(testEventsRdd))
        CassandraEventsSink(Option(dstream), session)
        testEventsRdd.saveToCassandra("fortis", "events", writeConf = WriteConf(ifNotExists = true))
        val addedEventsDF = session.sqlContext.read.format("org.apache.spark.sql.cassandra")
          .options(Map("keyspace" -> "fortis", "table" -> "eventbatches"))
          .load().select("*")

        addedEventsDF.createOrReplaceTempView("eventbatches")
        val cc = CassandraConnector(conf)

        val addedEvents = session.sqlContext.sql(s"select eventid, pipelinekey from eventbatches where batchid == '$batchid'")
        val testEventsDF = testEventsRdd.toDF
        val dedupedEvents = testEventsDF.join(addedEvents, Seq("eventid", "pipelinekey"))*/
>>>>>>> Rebasing
    }
}
