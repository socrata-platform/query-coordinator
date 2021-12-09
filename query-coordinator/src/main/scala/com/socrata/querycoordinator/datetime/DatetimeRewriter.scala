package com.socrata.querycoordinator.datetime

import scala.collection.{mutable => mu}
import com.socrata.soql.BinaryTree
import com.socrata.soql.ast.{ColumnOrAliasRef, Expression, FunctionCall, OrderBy, Select, Selection, StringLiteral}
import com.socrata.soql.functions.SoQLFunctions._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, LocalDateTime}

import java.util.TimeZone

object DatetimeRewriter {

  private val isoDateTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")

  private val truncateDateFormat = Map(
    FloatingTimeStampTruncYmd.name -> "yyyy-MM-dd",
    FloatingTimeStampTruncYm.name -> "yyyy-MM",
    FloatingTimeStampTruncY.name -> "yyyy"
  )

  def rewrite(selects: BinaryTree[Select]): (BinaryTree[Select], Option[DateTime]) = {
    implicit val datetimes = mu.Set[DateTime]()

    val rewritten = selects.map { select: Select =>
      rewrite(select)
    }

    val largestDatetime =
      if (datetimes.isEmpty) None
      else Some(datetimes.max(Ordering.fromLessThan((a: DateTime, b: DateTime) => a.isBefore(b))))

    (rewritten, largestDatetime )
  }

  private def rewrite(expr: Expression)(implicit datetimes: mu.Set[DateTime]): Expression = {
    expr match {
      case fc@FunctionCall(GetUtcDate.name, _, _, _) =>
        fixedNowAtUtc(fc)
      case fc@FunctionCall(truncateFn, Seq(
        FunctionCall(ToFloatingTimestamp.name, Seq(
          ColumnOrAliasRef(_, _),
          l@StringLiteral(_)
        ), _, _)
        ), _, _) if truncateDateFormat.contains(truncateFn) =>
        val dateTimeFormat = truncateDateFormat(truncateFn)
        floatingNowAtTimeZone(fc, l, dateTimeFormat)
      case fc@FunctionCall(_, _, Some(filter), _) =>
        fc.copy(filter = Some(rewrite(filter)(datetimes)))(fc.functionNamePosition, fc.position)
      case _ =>
        expr
    }
  }

  private def fixedNowAtUtc(fc: FunctionCall)(implicit datetimes: mu.Set[DateTime]): Expression = {
    val lastModified = new DateTime(DateTimeZone.UTC)
    val now = isoDateTimeFormat.print(lastModified)
    datetimes.add(lastModified)
    fc.copy(functionName = TextToFixedTimestamp.name,
            parameters = Seq(StringLiteral(now)(fc.position)))(fc.functionNamePosition, fc.position)
  }

  private def floatingNowAtTimeZone(fc: FunctionCall, timezone: StringLiteral, datetimeFormat: String)(implicit datetimes: mu.Set[DateTime]): Expression = {
    val tz =  TimeZone.getTimeZone(timezone.value)
    val dt = new DateTime(DateTimeZone.UTC)
    val daylightSaving = if (tz.inDaylightTime(dt.toDate)) tz.getDSTSavings else 0
    val dtAtTimezone = dt.plusMillis(tz.getRawOffset + daylightSaving)
    val fmt = DateTimeFormat.forPattern(datetimeFormat)
    val now = fmt.print(dtAtTimezone)
    val lastModified = DateTime.parse(now)
    datetimes.add(lastModified)
    fc.copy(functionName = TextToFloatingTimestamp.name,
            parameters = Seq(StringLiteral(now)(timezone.position)))(fc.functionNamePosition, fc.position)
  }

  private def rewrite(select: Select)(implicit datetimes: mu.Set[DateTime]): Select = {
    select.copy(
      selection = rewrite(select.selection),
      where = select.where.map(rewrite),
      groupBys = select.groupBys.map(rewrite),
      having = select.having.map(rewrite),
      orderBys = select.orderBys.map(rewrite)
    )
  }

  private def rewrite(selection: Selection)(implicit datetimes: mu.Set[DateTime]): Selection = {
    selection.copy(expressions = selection.expressions.map(se => se.copy(expression = rewrite(se.expression))))
  }

  private def rewrite(orderBy: OrderBy)(implicit datetimes: mu.Set[DateTime]): OrderBy = {
    orderBy.copy(expression = rewrite(orderBy.expression))
  }
}
