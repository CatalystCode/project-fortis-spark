package com.microsoft.partnercatalyst.fortis.spark.sources.streamfactories

import com.github.catalystcode.fortis.spark.streaming.rss._
import com.microsoft.partnercatalyst.fortis.spark.logging.Loggable
import com.microsoft.partnercatalyst.fortis.spark.sources.streamprovider.ConnectorConfig
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class RSSStreamFactory extends StreamFactoryBase[RSSEntry] with Loggable {

  override protected def canHandle(connectorConfig: ConnectorConfig): Boolean = {
    connectorConfig.name.equalsIgnoreCase("RSS")
  }

  override protected def buildStream(ssc: StreamingContext, connectorConfig: ConnectorConfig): DStream[RSSEntry] = {
    val params = connectorConfig.parameters
    connectorConfig.parameters.get("feedUrls") match {
      case Some(feedUrls:String) => {
        val urls = feedUrls.split("[|]")
        new RSSInputDStream(
          urls,
          storageLevel = StorageLevel.MEMORY_ONLY,
          requestHeaders = Map(
            "User-Agent" -> params.getOrElse("userAgent", "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36").toString
          ),
          connectTimeout = params.getOrElse("connectTimeout", "1000").toString.toInt,
          readTimeout = params.getOrElse("readTimeout", "3000").toString.toInt,
          pollingPeriodInSeconds = params.getOrElse("pollingPeriodInSeconds", "10").toString.toInt,
          ssc = ssc
        )
      }
      case _ => {
        throw new Exception("No feedUrls present for RSS feed stream $connectorConfig.")
      }
    }
  }

}