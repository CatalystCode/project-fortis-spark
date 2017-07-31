package com.microsoft.partnercatalyst.fortis.spark.analyzer.timeseries

object ComputedAnomalyKmeansModelKey {
  val Minute = s"kmeans.${PeriodType.Month.periodTypeName}.minute"
  val Hour = s"kmeans.${PeriodType.Month.periodTypeName}.hour"
  val DayOfWeek = s"kmeans.${PeriodType.Month.periodTypeName}.day_of_week"
  val DayOfMonth = s"kmeans.${PeriodType.Month.periodTypeName}.day_of_month"
  val DayOfYear = s"kmeans.${PeriodType.Month.periodTypeName}.day_of_year"
  val WeekOfMonth = s"kmeans.${PeriodType.Month.periodTypeName}.week_of_month"
  val WeekOfYear = s"kmeans.${PeriodType.Week.periodTypeName}.week_of_year"
  val Month = s"kmeans.${PeriodType.Month.periodTypeName}.month"
  val Year = s"kmeans.${PeriodType.Year.periodTypeName}.year"
}
