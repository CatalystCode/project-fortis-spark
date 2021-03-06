package com.microsoft.partnercatalyst.fortis.spark.analyzer.timeseries

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.scalatest.{BeforeAndAfter, FlatSpec}

class PeriodSpec extends FlatSpec with BeforeAndAfter {

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  format.setTimeZone(TimeZone.getTimeZone("UTC"))

  private var referenceDate: Date = _

  before {
    referenceDate = format.parse("2016-10-30 15:34:56")
  }

  it should "parse Minute period strings" in {
    val period = Period("minute-2016-10-30 15:34")
    assert(period.toString == "minute-2016-10-30 15:34")
  }

  it should "parse Hour period strings" in {
    val period = Period("hour-2016-10-30 15")
    assert(period.toString == "hour-2016-10-30 15")
  }

  it should "parse Day period strings" in {
    val period = Period("day-2016-10-30")
    assert(period.toString == "day-2016-10-30")
  }

  it should "parse Week period strings" in {
    val period = Period("week-2016-32")
    assert(period.toString == "week-2016-32")
    assert(format.format(new Date(period.startTime())) == "2016-07-31 00:00:00")
    assert(format.format(new Date(period.endTime())) == "2016-08-07 00:00:00")
  }

  it should "extract same week period from another period's start date" in {
    val referenceDate = format.parse("2017-09-11 15:34:56")
    val period = new Period(referenceDate.getTime, PeriodType.Week)
    assert(period.toString == "week-2017-37")

    val periodStartTime = period.startTime()
    val confirmation = new Period(periodStartTime, PeriodType.Week)
    assert(confirmation.toString == period.toString)
  }

  it should "parse Month period strings" in {
    val period = Period("month-2016-10")
    assert(period.toString == "month-2016-10")
  }

  it should "parse Year period strings" in {
    val period = Period("year-2016")
    assert(period.toString == "year-2016")
  }

  it should "format Minute to omit seconds" in {
    val date = format.parse("2016-07-04 08:15:45")
    val formattedDate = PeriodType.Minute.format(date.getTime)
    assert(formattedDate == "minute-2016-07-04 08:15")
  }

  it should "truncate Minute at second zero" in {
    val date = format.parse("2016-07-04 08:15:45")
    val truncatedDate = PeriodType.Minute.truncate(date.getTime)
    assert(format.format(truncatedDate) == "2016-07-04 08:15:00")
  }

  it should "format Hour to omit minutes and seconds" in {
    val date = format.parse("2016-07-04 08:15:45")
    val formattedDate = PeriodType.Hour.format(date.getTime)
    assert(formattedDate == "hour-2016-07-04 08")
  }

  it should "truncate Hour at minute zero" in {
    val date = format.parse("2016-07-04 08:15:32")
    val truncatedDate = PeriodType.Hour.truncate(date.getTime)
    assert(format.format(truncatedDate) == "2016-07-04 08:00:00")
  }

  it should "format Day to omit hour, minutes, and seconds" in {
    val date = format.parse("2016-07-04 08:15:45")
    val formattedDate = PeriodType.Day.format(date.getTime)
    assert(formattedDate == "day-2016-07-04")
  }

  it should "truncate Day to midnight" in {
    val date = format.parse("2016-07-04 08:15:32")
    val truncatedDate = PeriodType.Day.truncate(date.getTime)
    assert(format.format(truncatedDate) == "2016-07-04 00:00:00")
  }

  it should "format Month to contain year and month" in {
    val date = format.parse("2016-07-04 08:15:45")
    val formattedDate = PeriodType.Month.format(date.getTime)
    assert(formattedDate == "month-2016-07")
  }

  it should "truncate Month to first of month" in {
    val date = format.parse("2016-07-04 08:15:32")
    val truncatedDate = PeriodType.Month.truncate(date.getTime)
    assert(format.format(truncatedDate) == "2016-07-01 00:00:00")
  }

  it should "truncate Week to first of week" in {
    val date = format.parse("2016-07-05 08:15:32")
    val truncatedDate = PeriodType.Week.truncate(date.getTime)
    assert(format.format(truncatedDate) == "2016-07-03 00:00:00")
  }

  it should "format Year to contain a single element" in {
    val date = format.parse("2016-07-04 08:15:45")
    val formattedDate = PeriodType.Year.format(date.getTime)
    assert(formattedDate == "year-2016")
  }

  it should "truncate Year to January 1st" in {
    val date = format.parse("2016-07-04 08:15:32")
    val truncatedDate = PeriodType.Year.truncate(date.getTime)
    assert(format.format(truncatedDate) == "2016-01-01 00:00:00")
  }

  it should "iterate Year period stream" in {
    val from = format.parse("2010-02-01 08:15:32")
    val to = format.parse("2014-07-04 08:15:32")
    val periods = PeriodType.Year.periodsBetween(from.getTime, to.getTime).toSeq
    assert(periods == Seq("year-2010", "year-2011", "year-2012", "year-2013", "year-2014"))
  }

  "SequentialNestedSuiteExecution retrospectivePeriods" should "produce 4 hours from Minute." in {
    val periods = PeriodType.Minute.retrospectivePeriods(referenceDate.getTime)
    var expected = List("minute-2016-10-30 11:35", "minute-2016-10-30 11:36", "minute-2016-10-30 11:37", "minute-2016-10-30 11:38", "minute-2016-10-30 11:39", "minute-2016-10-30 11:40", "minute-2016-10-30 11:41", "minute-2016-10-30 11:42", "minute-2016-10-30 11:43", "minute-2016-10-30 11:44", "minute-2016-10-30 11:45", "minute-2016-10-30 11:46", "minute-2016-10-30 11:47", "minute-2016-10-30 11:48", "minute-2016-10-30 11:49", "minute-2016-10-30 11:50", "minute-2016-10-30 11:51", "minute-2016-10-30 11:52", "minute-2016-10-30 11:53", "minute-2016-10-30 11:54", "minute-2016-10-30 11:55", "minute-2016-10-30 11:56", "minute-2016-10-30 11:57", "minute-2016-10-30 11:58", "minute-2016-10-30 11:59", "minute-2016-10-30 12:00", "minute-2016-10-30 12:01", "minute-2016-10-30 12:02", "minute-2016-10-30 12:03", "minute-2016-10-30 12:04", "minute-2016-10-30 12:05", "minute-2016-10-30 12:06", "minute-2016-10-30 12:07", "minute-2016-10-30 12:08", "minute-2016-10-30 12:09", "minute-2016-10-30 12:10", "minute-2016-10-30 12:11", "minute-2016-10-30 12:12", "minute-2016-10-30 12:13", "minute-2016-10-30 12:14", "minute-2016-10-30 12:15", "minute-2016-10-30 12:16", "minute-2016-10-30 12:17", "minute-2016-10-30 12:18", "minute-2016-10-30 12:19", "minute-2016-10-30 12:20", "minute-2016-10-30 12:21", "minute-2016-10-30 12:22", "minute-2016-10-30 12:23", "minute-2016-10-30 12:24", "minute-2016-10-30 12:25", "minute-2016-10-30 12:26", "minute-2016-10-30 12:27", "minute-2016-10-30 12:28", "minute-2016-10-30 12:29", "minute-2016-10-30 12:30", "minute-2016-10-30 12:31", "minute-2016-10-30 12:32", "minute-2016-10-30 12:33", "minute-2016-10-30 12:34", "minute-2016-10-30 12:35", "minute-2016-10-30 12:36", "minute-2016-10-30 12:37", "minute-2016-10-30 12:38", "minute-2016-10-30 12:39", "minute-2016-10-30 12:40", "minute-2016-10-30 12:41", "minute-2016-10-30 12:42", "minute-2016-10-30 12:43", "minute-2016-10-30 12:44", "minute-2016-10-30 12:45", "minute-2016-10-30 12:46", "minute-2016-10-30 12:47", "minute-2016-10-30 12:48", "minute-2016-10-30 12:49", "minute-2016-10-30 12:50", "minute-2016-10-30 12:51", "minute-2016-10-30 12:52", "minute-2016-10-30 12:53", "minute-2016-10-30 12:54", "minute-2016-10-30 12:55", "minute-2016-10-30 12:56", "minute-2016-10-30 12:57", "minute-2016-10-30 12:58", "minute-2016-10-30 12:59", "minute-2016-10-30 13:00", "minute-2016-10-30 13:01", "minute-2016-10-30 13:02", "minute-2016-10-30 13:03", "minute-2016-10-30 13:04", "minute-2016-10-30 13:05", "minute-2016-10-30 13:06", "minute-2016-10-30 13:07", "minute-2016-10-30 13:08", "minute-2016-10-30 13:09", "minute-2016-10-30 13:10", "minute-2016-10-30 13:11", "minute-2016-10-30 13:12", "minute-2016-10-30 13:13", "minute-2016-10-30 13:14", "minute-2016-10-30 13:15", "minute-2016-10-30 13:16", "minute-2016-10-30 13:17", "minute-2016-10-30 13:18", "minute-2016-10-30 13:19", "minute-2016-10-30 13:20", "minute-2016-10-30 13:21", "minute-2016-10-30 13:22", "minute-2016-10-30 13:23", "minute-2016-10-30 13:24", "minute-2016-10-30 13:25", "minute-2016-10-30 13:26", "minute-2016-10-30 13:27", "minute-2016-10-30 13:28", "minute-2016-10-30 13:29", "minute-2016-10-30 13:30", "minute-2016-10-30 13:31", "minute-2016-10-30 13:32", "minute-2016-10-30 13:33", "minute-2016-10-30 13:34", "minute-2016-10-30 13:35", "minute-2016-10-30 13:36", "minute-2016-10-30 13:37", "minute-2016-10-30 13:38", "minute-2016-10-30 13:39", "minute-2016-10-30 13:40", "minute-2016-10-30 13:41", "minute-2016-10-30 13:42", "minute-2016-10-30 13:43", "minute-2016-10-30 13:44", "minute-2016-10-30 13:45", "minute-2016-10-30 13:46", "minute-2016-10-30 13:47", "minute-2016-10-30 13:48", "minute-2016-10-30 13:49", "minute-2016-10-30 13:50", "minute-2016-10-30 13:51", "minute-2016-10-30 13:52", "minute-2016-10-30 13:53", "minute-2016-10-30 13:54", "minute-2016-10-30 13:55", "minute-2016-10-30 13:56", "minute-2016-10-30 13:57", "minute-2016-10-30 13:58", "minute-2016-10-30 13:59", "minute-2016-10-30 14:00", "minute-2016-10-30 14:01", "minute-2016-10-30 14:02", "minute-2016-10-30 14:03", "minute-2016-10-30 14:04", "minute-2016-10-30 14:05", "minute-2016-10-30 14:06", "minute-2016-10-30 14:07", "minute-2016-10-30 14:08", "minute-2016-10-30 14:09", "minute-2016-10-30 14:10", "minute-2016-10-30 14:11", "minute-2016-10-30 14:12", "minute-2016-10-30 14:13", "minute-2016-10-30 14:14", "minute-2016-10-30 14:15", "minute-2016-10-30 14:16", "minute-2016-10-30 14:17", "minute-2016-10-30 14:18", "minute-2016-10-30 14:19", "minute-2016-10-30 14:20", "minute-2016-10-30 14:21", "minute-2016-10-30 14:22", "minute-2016-10-30 14:23", "minute-2016-10-30 14:24", "minute-2016-10-30 14:25", "minute-2016-10-30 14:26", "minute-2016-10-30 14:27", "minute-2016-10-30 14:28", "minute-2016-10-30 14:29", "minute-2016-10-30 14:30", "minute-2016-10-30 14:31", "minute-2016-10-30 14:32", "minute-2016-10-30 14:33", "minute-2016-10-30 14:34", "minute-2016-10-30 14:35", "minute-2016-10-30 14:36", "minute-2016-10-30 14:37", "minute-2016-10-30 14:38", "minute-2016-10-30 14:39", "minute-2016-10-30 14:40", "minute-2016-10-30 14:41", "minute-2016-10-30 14:42", "minute-2016-10-30 14:43", "minute-2016-10-30 14:44", "minute-2016-10-30 14:45", "minute-2016-10-30 14:46", "minute-2016-10-30 14:47", "minute-2016-10-30 14:48", "minute-2016-10-30 14:49", "minute-2016-10-30 14:50", "minute-2016-10-30 14:51", "minute-2016-10-30 14:52", "minute-2016-10-30 14:53", "minute-2016-10-30 14:54", "minute-2016-10-30 14:55", "minute-2016-10-30 14:56", "minute-2016-10-30 14:57", "minute-2016-10-30 14:58", "minute-2016-10-30 14:59", "minute-2016-10-30 15:00", "minute-2016-10-30 15:01", "minute-2016-10-30 15:02", "minute-2016-10-30 15:03", "minute-2016-10-30 15:04", "minute-2016-10-30 15:05", "minute-2016-10-30 15:06", "minute-2016-10-30 15:07", "minute-2016-10-30 15:08", "minute-2016-10-30 15:09", "minute-2016-10-30 15:10", "minute-2016-10-30 15:11", "minute-2016-10-30 15:12", "minute-2016-10-30 15:13", "minute-2016-10-30 15:14", "minute-2016-10-30 15:15", "minute-2016-10-30 15:16", "minute-2016-10-30 15:17", "minute-2016-10-30 15:18", "minute-2016-10-30 15:19", "minute-2016-10-30 15:20", "minute-2016-10-30 15:21", "minute-2016-10-30 15:22", "minute-2016-10-30 15:23", "minute-2016-10-30 15:24", "minute-2016-10-30 15:25", "minute-2016-10-30 15:26", "minute-2016-10-30 15:27", "minute-2016-10-30 15:28", "minute-2016-10-30 15:29", "minute-2016-10-30 15:30", "minute-2016-10-30 15:31", "minute-2016-10-30 15:32", "minute-2016-10-30 15:33", "minute-2016-10-30 15:34")
    assert(periods == expected)
  }

  it should "produce 7 days from Hour." in {
    val periods = PeriodType.Hour.retrospectivePeriods(referenceDate.getTime)
    val expected = List("hour-2016-10-23 16", "hour-2016-10-23 17", "hour-2016-10-23 18", "hour-2016-10-23 19", "hour-2016-10-23 20", "hour-2016-10-23 21", "hour-2016-10-23 22", "hour-2016-10-23 23", "hour-2016-10-24 00", "hour-2016-10-24 01", "hour-2016-10-24 02", "hour-2016-10-24 03", "hour-2016-10-24 04", "hour-2016-10-24 05", "hour-2016-10-24 06", "hour-2016-10-24 07", "hour-2016-10-24 08", "hour-2016-10-24 09", "hour-2016-10-24 10", "hour-2016-10-24 11", "hour-2016-10-24 12", "hour-2016-10-24 13", "hour-2016-10-24 14", "hour-2016-10-24 15", "hour-2016-10-24 16", "hour-2016-10-24 17", "hour-2016-10-24 18", "hour-2016-10-24 19", "hour-2016-10-24 20", "hour-2016-10-24 21", "hour-2016-10-24 22", "hour-2016-10-24 23", "hour-2016-10-25 00", "hour-2016-10-25 01", "hour-2016-10-25 02", "hour-2016-10-25 03", "hour-2016-10-25 04", "hour-2016-10-25 05", "hour-2016-10-25 06", "hour-2016-10-25 07", "hour-2016-10-25 08", "hour-2016-10-25 09", "hour-2016-10-25 10", "hour-2016-10-25 11", "hour-2016-10-25 12", "hour-2016-10-25 13", "hour-2016-10-25 14", "hour-2016-10-25 15", "hour-2016-10-25 16", "hour-2016-10-25 17", "hour-2016-10-25 18", "hour-2016-10-25 19", "hour-2016-10-25 20", "hour-2016-10-25 21", "hour-2016-10-25 22", "hour-2016-10-25 23", "hour-2016-10-26 00", "hour-2016-10-26 01", "hour-2016-10-26 02", "hour-2016-10-26 03", "hour-2016-10-26 04", "hour-2016-10-26 05", "hour-2016-10-26 06", "hour-2016-10-26 07", "hour-2016-10-26 08", "hour-2016-10-26 09", "hour-2016-10-26 10", "hour-2016-10-26 11", "hour-2016-10-26 12", "hour-2016-10-26 13", "hour-2016-10-26 14", "hour-2016-10-26 15", "hour-2016-10-26 16", "hour-2016-10-26 17", "hour-2016-10-26 18", "hour-2016-10-26 19", "hour-2016-10-26 20", "hour-2016-10-26 21", "hour-2016-10-26 22", "hour-2016-10-26 23", "hour-2016-10-27 00", "hour-2016-10-27 01", "hour-2016-10-27 02", "hour-2016-10-27 03", "hour-2016-10-27 04", "hour-2016-10-27 05", "hour-2016-10-27 06", "hour-2016-10-27 07", "hour-2016-10-27 08", "hour-2016-10-27 09", "hour-2016-10-27 10", "hour-2016-10-27 11", "hour-2016-10-27 12", "hour-2016-10-27 13", "hour-2016-10-27 14", "hour-2016-10-27 15", "hour-2016-10-27 16", "hour-2016-10-27 17", "hour-2016-10-27 18", "hour-2016-10-27 19", "hour-2016-10-27 20", "hour-2016-10-27 21", "hour-2016-10-27 22", "hour-2016-10-27 23", "hour-2016-10-28 00", "hour-2016-10-28 01", "hour-2016-10-28 02", "hour-2016-10-28 03", "hour-2016-10-28 04", "hour-2016-10-28 05", "hour-2016-10-28 06", "hour-2016-10-28 07", "hour-2016-10-28 08", "hour-2016-10-28 09", "hour-2016-10-28 10", "hour-2016-10-28 11", "hour-2016-10-28 12", "hour-2016-10-28 13", "hour-2016-10-28 14", "hour-2016-10-28 15", "hour-2016-10-28 16", "hour-2016-10-28 17", "hour-2016-10-28 18", "hour-2016-10-28 19", "hour-2016-10-28 20", "hour-2016-10-28 21", "hour-2016-10-28 22", "hour-2016-10-28 23", "hour-2016-10-29 00", "hour-2016-10-29 01", "hour-2016-10-29 02", "hour-2016-10-29 03", "hour-2016-10-29 04", "hour-2016-10-29 05", "hour-2016-10-29 06", "hour-2016-10-29 07", "hour-2016-10-29 08", "hour-2016-10-29 09", "hour-2016-10-29 10", "hour-2016-10-29 11", "hour-2016-10-29 12", "hour-2016-10-29 13", "hour-2016-10-29 14", "hour-2016-10-29 15", "hour-2016-10-29 16", "hour-2016-10-29 17", "hour-2016-10-29 18", "hour-2016-10-29 19", "hour-2016-10-29 20", "hour-2016-10-29 21", "hour-2016-10-29 22", "hour-2016-10-29 23", "hour-2016-10-30 00", "hour-2016-10-30 01", "hour-2016-10-30 02", "hour-2016-10-30 03", "hour-2016-10-30 04", "hour-2016-10-30 05", "hour-2016-10-30 06", "hour-2016-10-30 07", "hour-2016-10-30 08", "hour-2016-10-30 09", "hour-2016-10-30 10", "hour-2016-10-30 11", "hour-2016-10-30 12", "hour-2016-10-30 13", "hour-2016-10-30 14", "hour-2016-10-30 15")
    assert(periods == expected)
  }

  it should "produce 30 days from Day." in {
    val periods = PeriodType.Day.retrospectivePeriods(referenceDate.getTime)
    val expected = List("day-2016-10-01", "day-2016-10-02", "day-2016-10-03", "day-2016-10-04", "day-2016-10-05", "day-2016-10-06", "day-2016-10-07", "day-2016-10-08", "day-2016-10-09", "day-2016-10-10", "day-2016-10-11", "day-2016-10-12", "day-2016-10-13", "day-2016-10-14", "day-2016-10-15", "day-2016-10-16", "day-2016-10-17", "day-2016-10-18", "day-2016-10-19", "day-2016-10-20", "day-2016-10-21", "day-2016-10-22", "day-2016-10-23", "day-2016-10-24", "day-2016-10-25", "day-2016-10-26", "day-2016-10-27", "day-2016-10-28", "day-2016-10-29", "day-2016-10-30")
    assert(periods == expected)
  }

  it should "produce 6 months from Week." in {
    val periods = PeriodType.Week.retrospectivePeriods(referenceDate.getTime)
    val expected = List("week-2016-22", "week-2016-23", "week-2016-24", "week-2016-25", "week-2016-26", "week-2016-27", "week-2016-28", "week-2016-29", "week-2016-30", "week-2016-31", "week-2016-32", "week-2016-33", "week-2016-34", "week-2016-35", "week-2016-36", "week-2016-37", "week-2016-38", "week-2016-39", "week-2016-40", "week-2016-41", "week-2016-42", "week-2016-43", "week-2016-44", "week-2016-45")
    assert(periods == expected)
  }

  it should "produce 6 months from Month." in {
    val periods = PeriodType.Month.retrospectivePeriods(referenceDate.getTime)
    val expected = List("month-2013-11", "month-2013-12", "month-2014-01", "month-2014-02", "month-2014-03", "month-2014-04", "month-2014-05", "month-2014-06", "month-2014-07", "month-2014-08", "month-2014-09", "month-2014-10", "month-2014-11", "month-2014-12", "month-2015-01", "month-2015-02", "month-2015-03", "month-2015-04", "month-2015-05", "month-2015-06", "month-2015-07", "month-2015-08", "month-2015-09", "month-2015-10", "month-2015-11", "month-2015-12", "month-2016-01", "month-2016-02", "month-2016-03", "month-2016-04", "month-2016-05", "month-2016-06", "month-2016-07", "month-2016-08", "month-2016-09", "month-2016-10")
    assert(periods == expected)
  }

  it should "produce 7 years from Year." in {
    val periods = PeriodType.Year.retrospectivePeriods(referenceDate.getTime)
    val expected = List("year-2010", "year-2011", "year-2012", "year-2013", "year-2014", "year-2015", "year-2016")
    assert(periods == expected)
  }

}
