package com.socrata.querycoordinator.datetime

import com.socrata.querycoordinator.QueryParser.SuccessfulParse
import com.socrata.querycoordinator.TestBase
import org.joda.time.{DateTime, DateTimeZone}


class NowAnalyzerTest  extends TestBase {

  import com.socrata.querycoordinator.QueryParserTest._

  test("basic get_utc_date() returns now without ms") {
    val query = "SELECT a WHERE get_utc_date() is not null"
    val actual = qp.apply(query, truthColumns, upToDateSchema, fakeRequestBuilder, merged = false)
    actual shouldBe a[SuccessfulParse]

    val SuccessfulParse(analyses, _) = actual
    val nowWithMs = DateTime.now(DateTimeZone.UTC)
    val now = nowWithMs.minusMillis(nowWithMs.millisOfSecond.get)
    val result = (new NowAnalyzer(analyses).getNow()).get
    result.isBefore(now) shouldBe (false)
    result.millisOfSecond().get shouldBe (0)
  }

  test("get_utc_date() works with truncation and timezone translation") {
    val query = "SELECT date_trunc_ym(to_floating_timestamp(get_utc_date(), 'US/Pacific')) WHERE d > date_trunc_y(to_floating_timestamp(get_utc_date(), 'US/Pacific'))"
    val actual = qp.apply(query, truthColumns, upToDateSchema, fakeRequestBuilder, merged = false)
    actual shouldBe a[SuccessfulParse]
    val tz = DateTimeZone.forID("US/Pacific")
    val SuccessfulParse(analyses, _) = actual
    val now = DateTime.now(DateTimeZone.UTC).toDateTime(tz)
    val nowWODay = now.minusDays(now.dayOfMonth.get - 1).minusMillis(now.millisOfSecond.get).minusSeconds(now.secondOfDay.get)
    val result = (new NowAnalyzer(analyses).getNow).get
    result shouldBe (nowWODay)
  }

  test("get_utc_date() works with year truncation in order by") {
    val query = "SELECT d ORDER BY date_trunc_y(to_floating_timestamp(get_utc_date(), 'US/Eastern'))"
    val actual = qp.apply(query, truthColumns, upToDateSchema, fakeRequestBuilder, merged = false)
    actual shouldBe a[SuccessfulParse]
    val tz = DateTimeZone.forID("US/Eastern")
    val SuccessfulParse(analyses, _) = actual
    val nowWithMs = DateTime.now(DateTimeZone.UTC).toDateTime.toDateTime(tz)
    val now = nowWithMs.minusMillis(nowWithMs.millisOfSecond.get)
    val nowWODay = now.minusMonths(now.monthOfYear.get - 1).minusDays(now.dayOfMonth.get - 1).minusSeconds(now.secondOfDay.get)
    val result = (new NowAnalyzer(analyses).getNow).get
    result.getMonthOfYear shouldBe(1)
    result.getDayOfYear shouldBe(1)
    result.getHourOfDay shouldBe(0)
    result.getMinuteOfHour shouldBe(0)
    result.getSecondOfMinute shouldBe(0)
    result shouldBe(nowWODay)
  }

  test("get_utc_date() works with year truncation of fixed_timestamp") {
    val query = "SELECT datez_trunc_y(get_utc_date())"
    val actual = qp.apply(query, truthColumns, upToDateSchema, fakeRequestBuilder, merged = false)
    actual shouldBe a[SuccessfulParse]

    val SuccessfulParse(analyses, _) = actual
    val nowWithMs = DateTime.now(DateTimeZone.UTC)
    val now = nowWithMs.minusMillis(nowWithMs.millisOfSecond.get)
    val nowWODay = now.minusMonths(now.monthOfYear.get - 1).minusDays(now.dayOfMonth.get - 1).minusSeconds(now.secondOfDay.get)
    val result = (new NowAnalyzer(analyses).getNow).get
    result.getMonthOfYear shouldBe(1)
    result.getDayOfYear shouldBe(1)
    result.getHourOfDay shouldBe(0)
    result.getMinuteOfHour shouldBe(0)
    result.getSecondOfMinute shouldBe(0)

    val utc = result.withZone(DateTimeZone.UTC)
    result shouldBe (nowWODay)
    result shouldBe(utc)
  }
}
