package com.microsoft.partnercatalyst.fortis.spark

import java.util.{Date, UUID}

import com.microsoft.partnercatalyst.fortis.spark.dba.{CassandraConfigurationManager, ConfigurationManager}
import com.microsoft.partnercatalyst.fortis.spark.dto._
import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Properties.{envOrElse, envOrNone}

object CassandraTest {
  case class TestFortisEvent(
    details: Details,
    analysis: Analysis
  ) extends FortisEvent {
    override def copy(analysis: Analysis = null): FortisEvent = {
      TestFortisEvent(
        details = details,
        analysis = Option(analysis).getOrElse(this.analysis))
    }
  }

  case class TestFortisDetails(
    sourceeventid: String,
    eventid: String,
    eventtime: Long,
    body: String,
    imageurl: Option[String],
    externalsourceid: String,
    title: String,
    pipelinekey: String,
    sourceurl: String,
    sharedLocations: List[Location] = List()
  ) extends Details
  def envOrFail(name: String): String = {
    envOrNone(name) match {
      case Some(v) => v
      case None =>
        sys.error(s"Environment variable not defined: $name")
    }
  }

  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getSimpleName
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.cassandra.connection.host", envOrFail("FORTIS_CASSANDRA_HOST"))
      .setIfMissing("spark.cassandra.auth.username", envOrElse("FORTIS_CASSANDRA_USER", "cassandra"))
      .setIfMissing("spark.cassandra.auth.password", envOrElse("FORTIS_CASSANDRA_PASSWORD", "cassandra"))
      .set("spark.cassandra.input.consistency.level", "LOCAL_ONE")
      .set("spark.cassandra.output.consistency.level", "LOCAL_ONE")
      .set("output.consistency.level", "LOCAL_ONE")
      .setIfMissing("spark.master", "local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))
    val sparksession = SparkSession.builder().config(conf).getOrCreate()
    val batchid = UUID.randomUUID().toString

    val testEventsRdd = ssc.sparkContext.parallelize(Seq(TestFortisEvent(
      details = TestFortisDetails(
        eventtime = new Date().getTime,
        eventid = "235",
        sourceeventid = "original-235",
        sourceurl = "http://cnn.com",
        pipelinekey = "twitter",
        sharedLocations = List(),
        externalsourceid = "cnn",
        body = "test message a new change",
        imageurl = None,
        title = "twitter post" ),
      analysis = Analysis(
        sentiments = List(.5),
        locations = List(Location(wofId = "wof-85670485", name = "neverland", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1),
          Location(wofId = "wof-85670486", name = "neverlandI", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1),
          Location(wofId = "wof-85670487", name = "neverlandII", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1),
          Location(wofId = "wof-85670488", name = "neverlandIII", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1),
          Location(wofId = "wof-85670489", name = "neverlandIV", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1)),
        keywords = List(Tag(name = "isis", confidence = Option(1.0)), Tag(name ="car", confidence = Option(1.0)), Tag(name ="bomb", confidence = Option(1.0)), Tag(name ="hello", confidence = Option(1.0)), Tag(name ="team", confidence = Option(1.0))),
       //todo genders = List(Tag(name = "male", confidence = Option(1.0)), Tag(name ="female", confidence = Option(1.0))),
        entities = List(Tag(name = "putin", confidence = Option(1.0))),
        language = Option("en")
      )),
      TestFortisEvent(
        details = TestFortisDetails(
          eventtime = new Date().getTime,
          eventid = "434",
          sourceeventid = "original-434",
          sourceurl = "http://bbc.com",
          pipelinekey = "twitter",
          sharedLocations = List(),
          externalsourceid = "bbc",
          body = "This is a another test message",
          imageurl = None,
          title = "twitter post" ),
        analysis = Analysis(
          sentiments = List(.6),
          locations = List(Location(wofId = "wof-85670485", name = "neverland", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1),
            Location(wofId = "wof-85670486", name = "neverlandI", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1),
            Location(wofId = "wof-85670487", name = "neverlandII", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1),
            Location(wofId = "wof-85670488", name = "neverlandIII", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1),
            Location(wofId = "wof-85670489", name = "neverlandIV", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1)),
          keywords = List(Tag(name = "isis", confidence = Option(1.0)),
            Tag(name ="car", confidence = Option(1.0)),
            Tag(name ="bomb", confidence = Option(1.0)),
            Tag(name ="fatalities", confidence = Option(1.0))),
         //todo genders = List(Tag(name = "male", confidence = Option(1.0)), Tag(name ="female", confidence = Option(1.0))),
          entities = List(Tag(name = "putin", confidence = Option(1.0))),
          language = Option("en")
        )),
      TestFortisEvent(
        details = TestFortisDetails(
          eventtime = new Date().getTime,
          eventid = "435",
          sourceeventid = "original-435",
          sourceurl = "http://bloomberg.com",
          pipelinekey = "twitter",
          sharedLocations = List(),
          externalsourceid = "bloomberg",
          body = "This is a another test message from bloomberg",
          imageurl = None,
          title = "twitter post" ),
        analysis = Analysis(
          sentiments = List(.6),
          locations = List(Location(wofId = "wof-85670485", name = "neverland", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1),
            Location(wofId = "wof-85670486", name = "neverlandI", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1),
            Location(wofId = "wof-85670487", name = "neverlandII", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1),
            Location(wofId = "wof-85670488", name = "neverlandIII", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1),
            Location(wofId = "wof-85670489", name = "neverlandIV", layer = "country", confidence = Option(1.0), latitude = 12.21, longitude = 43.1)),
          keywords = List(Tag(name = "isis", confidence = Option(1.0)), Tag(name ="car", confidence = Option(1.0)), Tag(name ="bomb", confidence = Option(1.0)), Tag(name ="hello", confidence = Option(1.0)), Tag(name ="team", confidence = Option(1.0))),
          //todo genders = List(Tag(name = "male", confidence = Option(1.0)), Tag(name ="female", confidence = Option(1.0))),
          entities = List(Tag(name = "putin", confidence = Option(1.0))),
          language = Option("en")
        ))))

    val dstream = ssc.queueStream(mutable.Queue(testEventsRdd)).asInstanceOf[DStream[FortisEvent]]
    val configurationManager: ConfigurationManager = new CassandraConfigurationManager()
    CassandraEventsSink(dstream, sparksession, configurationManager)
    ssc.start()
    ssc.awaitTermination()
  }
}