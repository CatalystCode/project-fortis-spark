import java.lang.System.{getProperty, nanoTime, setProperty}

import com.datastax.driver.core.BoundStatement
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.Level.ERROR
import org.apache.log4j.Logger.getLogger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
CREATE TABLE fortis.demo(word text, count counter, PRIMARY KEY (word));
CREATE TABLE fortis.demo_processed(id text, PRIMARY KEY (id));
 */
object CounterDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CounterDemo")
    if (!sparkConf.contains("spark.master")) sparkConf.setMaster("local[2]")

    sparkConf.set("spark.cassandra.connection.host", getProperty("CASSANDRA_HOST"))
    sparkConf.set("spark.cassandra.auth.username", getProperty("CASSANDRA_USERNAME"))
    sparkConf.set("spark.cassandra.auth.password", getProperty("CASSANDRA_PASSWORD"))

    setProperty("twitter4j.oauth.consumerKey", getProperty("TWITTER_CONSUMER_KEY"))
    setProperty("twitter4j.oauth.consumerSecret", getProperty("TWITTER_CONSUMER_SECRET"))
    setProperty("twitter4j.oauth.accessToken", getProperty("TWITTER_ACCESS_TOKEN"))
    setProperty("twitter4j.oauth.accessTokenSecret", getProperty("TWITTER_ACCESS_TOKEN_SECRET"))

    getLogger("org").setLevel(ERROR)
    getLogger("akka").setLevel(ERROR)

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    implicit val connector: CassandraConnector = CassandraConnector(ssc.sparkContext)

    TwitterUtils.createStream(ssc, None)
    .map(tweet => {
      CassandraDto(tweet.getId.toString, tweet.getText.split(" "))
    })
    .filter(dto => {
      connector.withSessionDo(session => {
        val row = session.execute("SELECT id FROM fortis.demo_processed WHERE id = ? LIMIT 1", dto.id).one()
        val tweetIsNew = row == null
        if (!tweetIsNew) {
          println("====================================================================")
          println(s"= Skipping already ingested Tweet with id ${dto.id}")
          println("====================================================================")
        }
        tweetIsNew
      })
    })
    .foreachRDD(rdd => {
      val numEvents = rdd.count()
      if (numEvents == 0) {
        println("====================================================================")
        println("= Empty batch, nothing to do")
        println("====================================================================")
      } else {
        val t0 = nanoTime()
        rdd.foreachPartition(dtos => {
          connector.withSessionDo(session => {
            val insert = new BoundStatement(session.prepare("INSERT INTO fortis.demo_processed (id) VALUES (?)"))
            val update = new BoundStatement(session.prepare("UPDATE fortis.demo SET count = count + 1 WHERE word = ?"))
            dtos.foreach(dto => {
              dto.words.foreach(word => {
                session.executeAsync(update.bind(word))
              })
              session.executeAsync(insert.bind(dto.id))
            })
          })
        })
        val t1 = nanoTime()
        println("====================================================================")
        println(s"= Total cassandra write time for $numEvents is ${(t1 - t0).toDouble / 1000000000} seconds")
        println("====================================================================")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

case class CassandraDto(id: String, words: Array[String])
