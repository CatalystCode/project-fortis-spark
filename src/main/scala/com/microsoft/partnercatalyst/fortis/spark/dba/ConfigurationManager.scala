package com.microsoft.partnercatalyst.fortis.spark.dba

import com.microsoft.partnercatalyst.fortis.spark.dto.{BlacklistedTerm, SiteSettings}
import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.dto.TrustedSource
import com.microsoft.partnercatalyst.fortis.spark.sources.streamprovider.ConnectorConfig
import org.apache.spark.SparkContext

trait ConfigurationManager {
  def fetchConnectorConfigs(sparkContext: SparkContext, pipeline: String): List[ConnectorConfig]
  def fetchSiteSettings(sparkContext: SparkContext): SiteSettings

  def fetchWatchlist(sparkContext: SparkContext): Map[String, Seq[String]]
  def fetchBlacklist(sparkContext: SparkContext): Seq[BlacklistedTerm]
}
