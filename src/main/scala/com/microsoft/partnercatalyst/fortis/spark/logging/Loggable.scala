package com.microsoft.partnercatalyst.fortis.spark.logging

import java.io.{PrintWriter, StringWriter}
import java.util

import com.microsoft.applicationinsights.telemetry.Duration
import com.microsoft.applicationinsights.{TelemetryClient, TelemetryConfiguration}
import org.apache.log4j.LogManager

trait Loggable {
  @transient private lazy val log = LogManager.getLogger(getClass.getName)
  @transient private val appInsights: TelemetryClient = new TelemetryClient(TelemetryConfiguration.createDefault())

  def logDebug(message: String): Unit = {
    if (!log.isDebugEnabled) return

    log.debug(message)
    appInsights.trackTrace(message)
  }

  def logInfo(message: String): Unit = {
    if (!log.isInfoEnabled) return

    log.info(message)
    appInsights.trackTrace(message)
  }

  def logError(message: String): Unit = {
    log.error(message)
    appInsights.trackTrace(message)
  }

  def logError(message: String, throwable: Throwable): Unit = {
    log.error(message, throwable)
    appInsights.trackTrace(s"$message\n${stringify(throwable)}")
  }

  def logFatalError(message: String): Unit = {
    log.fatal(message)
    appInsights.trackTrace(message)
  }

  def logFatalError(message: String, throwable: Throwable): Unit = {
    log.fatal(message, throwable)
    appInsights.trackTrace(s"$message\n${stringify(throwable)}")
  }

  def logDependency(name: String, operation: String, success: Boolean, runtimeMillis: Long = 0L): Unit = {
    appInsights.trackDependency(name, operation, new Duration(runtimeMillis), success)
  }

  def logEvent(name: String, properties: Map[String, String] = Map(), metrics: Map[String, Double] = Map()): Unit = {
    val properties_ = new util.HashMap[java.lang.String, java.lang.String](properties.size)
    properties.foreach(kv => properties_.put(kv._1, kv._2))

    val metrics_ = new util.HashMap[java.lang.String, java.lang.Double](metrics.size)
    metrics.foreach(kv => metrics_.put(kv._1, kv._2))

    appInsights.trackEvent(name, properties_, metrics_)
  }

  private def stringify(throwable: Throwable): String = {
    if (throwable == null) return ""

    val stringWriter = new StringWriter()
    throwable.printStackTrace(new PrintWriter(stringWriter))
    stringWriter.toString
  }
}
