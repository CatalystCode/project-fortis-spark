package com.microsoft.partnercatalyst.fortis.spark.logging

import java.io.{PrintWriter, StringWriter}

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

  private def stringify(throwable: Throwable): String = {
    if (throwable == null) return ""

    val stringWriter = new StringWriter()
    throwable.printStackTrace(new PrintWriter(stringWriter))
    stringWriter.toString
  }
}
