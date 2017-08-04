package com.microsoft.partnercatalyst.fortis.spark.statistics

import java.time.{ZoneId, ZonedDateTime}

import com.microsoft.partnercatalyst.fortis.spark.analyzer.timeseries.PeriodType
import com.microsoft.partnercatalyst.fortis.spark.dto.TopicCount

case class ComputedAnomalyKmeansModelParameter(
  key: String,
  k: Int,
  maxIter: Int,
  periodFeature: TopicCount => Double
) extends Serializable

object ComputedAnomalyKmeansModelParameter {

  val parametersForAnalysis: Set[ComputedAnomalyKmeansModelParameter] = PeriodType.all.flatMap(kmeansParameters(_, 1, 1))

  val parametersForAnalysisByKey: Map[String, ComputedAnomalyKmeansModelParameter] = parametersForAnalysis.groupBy(_.key).mapValues(v=>v.head)

  def kmeansParameters(periodType: PeriodType,
                       uniqueSourcesCount: Int,
                       uniqueTopicsCount: Int): Seq[ComputedAnomalyKmeansModelParameter] = {
    periodType match {
      case PeriodType.Minute => Seq(
        ComputedAnomalyKmeansModelParameter(
          ComputedAnomalyKmeansModelKey.Minute,
          60 * uniqueSourcesCount * uniqueTopicsCount, /* base of 60 buckets representing each minute within an hour */
          20,
          topicCount => {
            100 * /* spacing between minute features */
              ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getMinute
          })
      )
      case PeriodType.Hour => Seq(
        ComputedAnomalyKmeansModelParameter(
          ComputedAnomalyKmeansModelKey.Hour,
          24 * uniqueSourcesCount * uniqueTopicsCount, /* base of 24 buckets representing each hour a day */
          20,
          topicCount => {
            1000 * /* spacing between hour features */
              ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getHour
          })
      )
      case PeriodType.Day => Seq(
        ComputedAnomalyKmeansModelParameter(
          ComputedAnomalyKmeansModelKey.DayOfWeek,
          7 * uniqueSourcesCount * uniqueTopicsCount, /* base of 7 buckets representing each day of the week */
          20,
          topicCount => {
            100000 * /* spacing between day of week features */
              ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getDayOfWeek.getValue
          }),
        ComputedAnomalyKmeansModelParameter(
          ComputedAnomalyKmeansModelKey.DayOfMonth,
          31 * uniqueSourcesCount * uniqueTopicsCount, /* base of 31 buckets representing the max number of days in a month */
          20,
          topicCount => {
            100000 * /* spacing between day of month features */
              ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getDayOfMonth
          }),
        ComputedAnomalyKmeansModelParameter(
          ComputedAnomalyKmeansModelKey.DayOfYear,
          365 * uniqueSourcesCount * uniqueTopicsCount, /* base of 365 buckets representing number of days in a non-leap year  */
          20,
          topicCount => {
            100000 * /* spacing between day of year features */
              ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getDayOfYear
          })
      )
      case PeriodType.Week => Seq(
        ComputedAnomalyKmeansModelParameter(
          ComputedAnomalyKmeansModelKey.WeekOfMonth,
          4 * uniqueSourcesCount * uniqueTopicsCount, /* base of 4 buckets representing weeks in a month */
          20,
          topicCount => {
            100000 * /* spacing between week of month features */
              ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getDayOfMonth / 7
          }),
        ComputedAnomalyKmeansModelParameter(
          ComputedAnomalyKmeansModelKey.WeekOfYear,
          52 * uniqueSourcesCount * uniqueTopicsCount, /* base of 52 buckets representing weeks in a non-leap year */
          20,
          topicCount => {
            100000 * /* spacing between week of year features */
              ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getDayOfYear / 52
          })
      )
      case PeriodType.Month => Seq(
        ComputedAnomalyKmeansModelParameter(
          ComputedAnomalyKmeansModelKey.Month,
          12 * uniqueSourcesCount * uniqueTopicsCount, /* base of 12 buckets representing months in a year */
          20,
          topicCount => {
            1000000 * /* spacing between week of year features */
              ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getMonthValue
          })
      )
      case PeriodType.Year => Seq(
        ComputedAnomalyKmeansModelParameter(
          ComputedAnomalyKmeansModelKey.Year,
          12 * uniqueSourcesCount * uniqueTopicsCount, /* Analyze a little over a decade to catch things like election cycles */
          20,
          topicCount => {
            1000000 * /* spacing between year features */
              ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(topicCount.periodstartdate), ZoneId.of("UTC")).getMonthValue
          })
      )
    }
  }

}
