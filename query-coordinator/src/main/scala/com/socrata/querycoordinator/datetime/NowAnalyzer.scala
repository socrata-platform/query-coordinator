package com.socrata.querycoordinator.datetime

import com.socrata.soql.functions.MonomorphicFunction
import com.socrata.soql.{BinaryTree, SoQLAnalysis}
import com.socrata.soql.typed._
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.typed.CoreExpr
import org.joda.time.{DateTime, DateTimeZone}


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
      case FunctionCall(MonomorphicFunction(truncateFn, _), Seq(
             FunctionCall(MonomorphicFunction(toFloatingTimestampFn, _), Seq(
               FunctionCall(MonomorphicFunction(getUtcDateFn, _), _, _, _),
               l@StringLiteral(_, _)), _, _)), _, _)
        if getUtcDateFn.name == GetUtcDate.name &&
           truncateDateFns.contains(truncateFn.name) &&
           toFloatingTimestampFn.name == ToFloatingTimestamp.name =>
        nowAtTimeZone(truncateFn)
      case FunctionCall(MonomorphicFunction(truncateFn, _), Seq(
             FunctionCall(MonomorphicFunction(getUtcDateFn, _), _, _, _)), _, _)
        if getUtcDateFn.name == GetUtcDate.name &&
          truncateDateFns.contains(truncateFn.name) =>
        nowAtTimeZone(truncateFn)
      case FunctionCall(MonomorphicFunction(fn, _), _, _, _) if fn.name == GetUtcDate.name =>
        nowAtTimeZone(GetUtcDate)
      case FunctionCall(_, args, filter, _) =>
        args.flatMap(collectNow) ++ filter.toSeq.flatMap(collectNow)
      case _ =>
        Seq.empty
    }
  }

  /**
    * return now in datetime with year, month, day or second precision depending on the date_trunc/get_utc_date function
    */
  private def nowAtTimeZone(dateTrunc: com.socrata.soql.functions.Function[_]): Seq[DateTime] = {
    val now = new DateTime(DateTimeZone.UTC)
    val nowTruncated = dateTrunc match {
      case GetUtcDate =>
        now.minusMillis(now.millisOfSecond.get)
      case FloatingTimeStampTruncYmd | FixedTimeStampZTruncYmd =>
        now.minusMillis(now.millisOfDay.get)
      case FloatingTimeStampTruncYm | FixedTimeStampZTruncYm =>
        now.minusDays(now.dayOfMonth.get - 1).minusMillis(now.millisOfDay.get)
      case FloatingTimeStampTruncY | FixedTimeStampZTruncY =>
        now.minusDays(now.dayOfYear.get - 1).minusMillis(now.millisOfDay.get)
      case unknownFn =>
        throw new Exception(s"should never get other function ${unknownFn.name.name}")
    }
    Seq(nowTruncated)
  }
}

object NowAnalyzer {
  private val truncateDateFns = Set(
    FloatingTimeStampTruncYmd,
    FloatingTimeStampTruncYm,
    FloatingTimeStampTruncY,
    FixedTimeStampZTruncYmd,
    FixedTimeStampZTruncYm,
    FixedTimeStampZTruncY
  ).map(_.name)
}
