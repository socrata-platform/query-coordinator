package com.socrata.querycoordinator.datetime

import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.{BinaryTree, SoQLAnalysis}
import com.socrata.soql.typed._
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.typed.CoreExpr
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

import java.util.TimeZone

/**
  *  Look for all uses of get_utc_date/now in a query and return the largest value.
  *  More precision means larger value.  For example, truncating at day is larger than truncating at month.
  *  The value is put in query context variable ":now": epoch(millseconds).
  *  The value is also included in the latest modified date of datasets involved
  *  in the calculation of Last-Modified http request header
  *  The above two acts should work well with ETAG and If-Modified-Since in caching
  */
class NowAnalyzer[A, B](selects: BinaryTree[SoQLAnalysis[A, B]]) {

  import NowAnalyzer._

  /**
    * Return the largest now or None if none exists
    */
  def getNow(): Option[DateTime] = {
    val nows = selects.seq.flatMap(collectNow)
    if (nows.isEmpty) None
    else Some(nows.max(Ordering.fromLessThan((a: DateTime, b: DateTime) => a.isBefore(b))))
  }

  private def collectNow(analysis: SoQLAnalysis[A, B]): Seq[DateTime] = {
    analysis.selection.values.toSeq.flatMap(collectNow) ++
      analysis.where.toSeq.flatMap(collectNow) ++
      analysis.groupBys.flatMap(collectNow) ++
      analysis.having.toSeq.flatMap(collectNow) ++
      analysis.orderBys.flatMap { x => collectNow(x.expression) }
  }

  private def collectNow(expr: CoreExpr[A, B]): Seq[DateTime] = {
    expr match {
      case FunctionCall(MonomorphicFunction(fn, _), _, _, _) if fn.name == GetUtcDate.name =>
        fixedNowAtUtc()
      case FunctionCall(MonomorphicFunction(truncateFn, _), Seq(
             FunctionCall(MonomorphicFunction(toFloatingTimestampFn, _), Seq(
               FunctionCall(MonomorphicFunction(getUtcDateFn, _), _, _, _),
               l@StringLiteral(_, _)), _, _)), _, _)
        if getUtcDateFn.name == GetUtcDate.name &&
           truncateDateFormat.contains(truncateFn.name) &&
           toFloatingTimestampFn.name == ToFloatingTimestamp.name =>
        val dateTimeFormat = truncateDateFormat(truncateFn.name)
        floatingNowAtTimeZone(l, dateTimeFormat)
      case FunctionCall(_, args, filter, _) =>
        args.flatMap(collectNow) ++ filter.toSeq.flatMap(collectNow)
      case _ =>
        Seq.empty
    }
  }

  /**
    * return now in datetime with second precision
    */
  private def fixedNowAtUtc(): Seq[DateTime] = {
    val nowMs = new DateTime(DateTimeZone.UTC)
    val now = nowMs.minusMillis(nowMs.getMillisOfSecond) // Remove milliseconds precision
    Seq(now)
  }

  /**
    * return now in datetime with year, month or day precision depending on datetimeFormat
    */
  private def floatingNowAtTimeZone(timezone: StringLiteral[B], datetimeFormat: String): Seq[DateTime] = {
    val tz =  TimeZone.getTimeZone(timezone.value)
    val dt = new DateTime(DateTimeZone.UTC)
    val daylightSaving = if (tz.inDaylightTime(dt.toDate)) tz.getDSTSavings else 0
    val dtAtTimezone = dt.plusMillis(tz.getRawOffset + daylightSaving)
    val fmt = DateTimeFormat.forPattern(datetimeFormat)
    val now = fmt.print(dtAtTimezone)
    Seq(DateTime.parse(now))
  }
}

object NowAnalyzer {
  private val truncateDateFormat = Map(
    FloatingTimeStampTruncYmd.name -> "yyyy-MM-dd",
    FloatingTimeStampTruncYm.name -> "yyyy-MM",
    FloatingTimeStampTruncY.name -> "yyyy"
  )
}
