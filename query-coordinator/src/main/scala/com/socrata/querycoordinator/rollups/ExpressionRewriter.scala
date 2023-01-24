package com.socrata.querycoordinator.rollups

import com.socrata.soql._
import com.socrata.soql.functions._
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.types._
import com.socrata.querycoordinator.rollups.ExpressionRewriter._
import com.socrata.querycoordinator.rollups.QueryRewriter.{Analysis, Expr, ColumnId}
import com.socrata.querycoordinator.rollups.QueryRewriterImplementation._
import com.socrata.soql.typed.{ColumnRef => _, FunctionCall => _, Join => _, OrderBy => _, _}

import org.joda.time.{DateTimeConstants, LocalDateTime}

class ExpressionRewriter(val rollupColumnId: (Int) => String,
                         val rewriteWhere: (Option[Expr], Analysis, Map[Expr, Int]) => Option[Where]) {

  import com.socrata.querycoordinator.util.Join._


  val log = org.slf4j.LoggerFactory.getLogger(classOf[ExpressionRewriter])

  /** Recursively maps the Expr based on the rollupColIdx map, returning either
    * a mapped expression or None if the expression couldn't be mapped.
    *
    * Note that every case here needs to ensure to map every expression recursively
    * to ensure it is either a literal or mapped to the rollup.
    */
  def apply(e: Expr, r: Analysis, rollupColIdx: Map[Expr, Int]): Option[Expr] = {
    log.trace("Attempting to match expr: {}", e)
    e match {
      case literal: typed.TypedLiteral[_] =>
        Some(literal) // This is literally a literal, so so literal.
      // for a column reference we just need to map the column id
      case cr: ColumnRef =>
        for {idx <- rollupColIdx.get(cr)} yield cr.copy(qualifier = None, column = rollupColumnId(idx))
      // window functions run after where / group by / having so can't be rewritten in isolation.  We could
      // still rewrite a query with a window function in if the other clauses all match, however that requires
      // coordination between expression mapping and mapping other clauses at a higher level that isn't implemented,
      // so for now we just forbid it entirely to avoid incorrect rewrites.
      case fc: FunctionCall if fc.window.nonEmpty =>
        None
      // A count(*) or count(non-null-literal) on q gets mapped to a sum on any such column in rollup
      case fc: FunctionCall if isCountStarOrLiteral(fc) =>
        rewriteCountStarOrLiteral(r, rollupColIdx, fc)
      // A count(...) on q gets mapped to a sum(...) on a matching column in the rollup.  We still need the count(*)
      // case above to ensure we can do things like map count(1) and count(*) which aren't exact matches.
      case fc@typed.FunctionCall(MonomorphicFunction(Count, _), _, filter, window) =>
        rewriteCount(r, rollupColIdx, fc, filter, window)
      // An avg(...) on q gets mapped to a sum(...)/count(...) on a matching column in the rollup
      case fc@typed.FunctionCall(MonomorphicFunction(Avg, _), _, filter, window) =>
        rewriteAvg(r, rollupColIdx, fc, filter, window)
      // If this is a between function operating on floating timestamps, and arguments b and c are both date aggregates,
      // then try to rewrite argument a to use a rollup.
      case fc: FunctionCall
        if (fc.function.function == Between || fc.function.function == NotBetween) &&
          fc.function.bindings.values.forall(_ == SoQLFloatingTimestamp) &&
          fc.function.bindings.values.tail.forall(dateTruncHierarchy contains _) =>
        rewriteDateTruncBetween(r, rollupColIdx, fc)
      // If it is a >= or < with floating timestamp arguments, see if we can rewrite to date_trunc_xxx
      case fc@typed.FunctionCall(MonomorphicFunction(fnType, bindings), _, _, _)
        if (fnType == Gte || fnType == Lt) &&
          bindings.values.forall(_ == SoQLFloatingTimestamp) =>
        rewriteDateTruncGteLt(r, rollupColIdx, fc)
      // Not null on a column can be translated to not null on a date_trunc_xxx(column)
      // There is actually a much more general case on this where non-aggregate functions can
      // be applied on top of other non-aggregate functions in many cases that we are not currently
      // implementing.
      case fc@typed.FunctionCall(MonomorphicFunction(IsNotNull, _), Seq(colRef@typed.ColumnRef(_, _, _)), filter, window)
        if findFunctionOnColumn(rollupColIdx, dateTruncHierarchy, colRef).isDefined =>
        for {
          colIdx <- findFunctionOnColumn(rollupColIdx, dateTruncHierarchy, colRef)
        } yield fc.copy(
          parameters = Seq(typed.ColumnRef(NoQualifier, rollupColumnId(colIdx), colRef.typ)(fc.position)),
          window = window)
      case fc: FunctionCall if dateTruncHierarchy.exists(dtf => dtf == fc.function.function) =>
        rewriteDateTrunc(r, rollupColIdx, fc)
      // If the function is not aggregate expression, and we have an exact rollup column
      // then just replace the function with column
      case fc: FunctionCall if !isAggregateExpression(fc) && rollupColIdx.contains(fc) =>
        Some(typed.ColumnRef(NoQualifier, rollupColumnId(rollupColIdx(fc)), fc.typ)(fc.position))
      // If the function is "self aggregatable" we can apply it on top of an already aggregated rollup
      // column, eg. select foo, bar, max(x) max_x group by foo, bar --> select foo, max(max_x) group by foo
      // If we have a matching column, we just need to update its argument to reference the rollup column.
      case fc: FunctionCall if isSelfAggregatableAggregate(fc, r, rollupColIdx) =>
        rewriteSelfAggregatableAggregate(r, rollupColIdx, fc)
      // At the end, just try to rewrite the function parameters
      case fc: FunctionCall =>
        rewriteParameters(r, rollupColIdx, fc)
      case _ =>
        None
    }
  }

  /*
   *  Count star or literal
   */
  private def rewriteCountStarOrLiteral(r: Analysis, rollupColIdx: Map[Expr, Int], fc: FunctionCall): Option[Expr] = {
    val filterAndRollupWhere = andRollupWhereToFilter(fc.filter, r: Analysis)
    for {
      idx <- findCountStarOrLiteral(rollupColIdx) // find count(*) column in rollup
      rwFilter <- rewriteWhere(filterAndRollupWhere, r, rollupColIdx)
    } yield {
      val simplifiedRwFilter = simplifyAndTrue(rwFilter)
      val newSumCol = typed.ColumnRef(NoQualifier, rollupColumnId(idx), SoQLNumber.t)(fc.position)
      typed.FunctionCall(CoalesceNumber, Seq(typed.FunctionCall(SumNumber, Seq(newSumCol), simplifiedRwFilter, fc.window)(fc.position, fc.position),
        typed.NumberLiteral(0, SoQLNumber.t)(fc.position)), None, fc.window)(fc.position, fc.position)
    }
  }

  /** Find the first column index that is a count(*) or count(literal). */
  private def findCountStarOrLiteral(rollupColIdx: Map[Expr, Int]): Option[Int] = {
    rollupColIdx.find {
      case (fc: FunctionCall, _) => isCountStarOrLiteral(fc)
      case _ => false
    }.map(_._2)
  }

  /** Is the given function call a count(*) or count(literal), excluding NULL because it is special. */
  private def isCountStarOrLiteral(fc: FunctionCall): Boolean = {
    fc.function.function == CountStar ||
      (fc.function.function == Count &&
        fc.parameters.forall {
          case e: typed.NullLiteral[_] => false
          case e: typed.TypedLiteral[_] => true
          case _ => false
        })
  }

  /*
   *  Count
   */
  private def rewriteCount(r: Analysis,
                           rollupColIdx: Map[Expr, Int],
                           fc: FunctionCall,
                           filter: Option[CoreExpr[ColumnId, SoQLType]],
                           window: Option[WindowFunctionInfo[ColumnId, SoQLType]]): Option[Expr] = {
    val fcMinusFilter = stripFilter(fc)
    val filterAndRollupWhere = andRollupWhereToFilter(filter, r: Analysis)
    for {
      idx <- rollupColIdx.get(fcMinusFilter) // find count(...) in rollup that matches exactly
      rwFilter <- rewriteWhere(filterAndRollupWhere, r, rollupColIdx)
    } yield {
      val simplifiedRwFilter = simplifyAndTrue(rwFilter)
      val newSumCol = typed.ColumnRef(NoQualifier, rollupColumnId(idx), SoQLNumber.t)(fc.position)
      typed.FunctionCall(CoalesceNumber, Seq(typed.FunctionCall(SumNumber, Seq(newSumCol), simplifiedRwFilter, window)(fc.position, fc.position),
        typed.NumberLiteral(0, SoQLNumber.t)(fc.position)), None, window)(fc.position, fc.position)
    }
  }

  /*
   *  Avg
   */
  private def rewriteAvg(r: Analysis,
                           rollupColIdx: Map[Expr, Int],
                           fc: FunctionCall,
                           filter: Option[CoreExpr[ColumnId, SoQLType]],
                           window: Option[WindowFunctionInfo[ColumnId, SoQLType]]): Option[Expr] = {
    val sumMinusFilter = stripFilter(fc).copy(function = SumNumber)
    val countMinusFilter = stripFilter(fc).copy(function = CountNumber)
    val filterAndRollupWhere = andRollupWhereToFilter(filter, r)
    for {
      sumIdx <- rollupColIdx.get(sumMinusFilter)
      countIdx <- rollupColIdx.get(countMinusFilter)
      rwFilter <- rewriteWhere(filterAndRollupWhere, r, rollupColIdx)
    } yield {
      val simplifiedRwFilter = simplifyAndTrue(rwFilter)
      val newSumCol = typed.ColumnRef(NoQualifier, rollupColumnId(sumIdx), SoQLNumber.t)(fc.position)
      val newCountCol = typed.ColumnRef(NoQualifier, rollupColumnId(countIdx), SoQLNumber.t)(fc.position)
      typed.FunctionCall(DivNumber,
        Seq(typed.FunctionCall(SumNumber, Seq(newSumCol), simplifiedRwFilter, window)(fc.position, fc.position),
          typed.FunctionCall(CountNumber, Seq(newCountCol), simplifiedRwFilter, window)(fc.position, fc.position)),
        None, window)(fc.position, fc.position)
    }
  }

  /*
   * Self aggregatable aggregate
   */
  private def rewriteSelfAggregatableAggregate(r: Analysis, rollupColIdx: Map[Expr, Int], fc: FunctionCall): Option[Expr] = {
    val fcMinusFilter = stripFilter(fc)
    val filterAndRollupWhere = andRollupWhereToFilter(fc.filter, r: Analysis)

    for {
      idx <- rollupColIdx.get(fcMinusFilter)
      rwFilter <- rewriteWhere(filterAndRollupWhere, r, rollupColIdx)
    } yield {
      val simplifiedRwFilter = simplifyAndTrue(rwFilter)
      fc.copy(parameters = Seq(typed.ColumnRef(NoQualifier, rollupColumnId(idx), fc.typ)(fc.position)),
        filter = simplifiedRwFilter)
    }
  }

  /** Can this function be applied to its own output in a further aggregation */
  private def isSelfAggregatableAggregate(fc: FunctionCall, r: Analysis, rollupColIdx: Map[Expr, Int]): Boolean = {
    fc.function.function match {
      case Max | Min | Sum
        if rollupColIdx.contains(stripFilter(fc)) &&
          rewriteWhere(andRollupWhereToFilter(fc.filter, r: Analysis), r, rollupColIdx) != None =>
        true
      case _ =>
        false
    }
  }

  private def stripFilter(fc: FunctionCall): FunctionCall = {
    fc.filter match {
      case Some(_) => fc.copy(filter = None)
      case None => fc
    }
  }

  private def andRollupWhereToFilter(filter: Option[Expr], r: Analysis): Option[Expr] = {
    (r.where, filter) match {
      case (None, _) => filter
      case (s@Some(_), None) => s
      case (Some(rw), Some(f)) =>
        Some(typed.FunctionCall(SoQLFunctions.And.monomorphic.get, List(rw, f), None, None)(rw.position, rw.position))
    }
  }

  def truncatedTo(soqlTs: SoQLFloatingTimestamp)
  : Option[Function[SoQLType]] = {
    val ts: LocalDateTime = soqlTs.value
    if (ts.getMillisOfDay != 0) {
      None
    } else if (ts.getDayOfMonth != 1) {
      Some(FloatingTimeStampTruncYmd)
    } else if (ts.getMonthOfYear != DateTimeConstants.JANUARY) {
      Some(FloatingTimeStampTruncYm)
    } else {
      Some(FloatingTimeStampTruncY)
    }
  }

  def isConstant(expr: Expr): Boolean = {
    expr match {
      case _: ColumnRef =>
        false
      case _: TypedLiteral[_] =>
        true
      case fc: FunctionCall =>
        !fc.function.isAggregate &&
          fc.parameters.nonEmpty &&
          fc.parameters.forall(x => isConstant(x))
      case _ =>
        false
    }
  }

  /** The common date truncation function shared between the lower and upper bounds of the between */
  private def commonDateTrunc[T, U](lower: Expr,
                                    upper: Expr): Option[Function[SoQLType]] = {
    (lower, upper) match {
      case (lowerFc: FunctionCall, upperFc: FunctionCall) =>
        (lowerFc.function.function, upperFc.function.function) match {
          case (l, u) if l == u && dateTruncHierarchy.contains(l) => Some(l)
          case _ => None
        }
      case _ => None
    }
  }

  private def isAggregateExpression(e: Expr): Boolean = {
    e match {
      case fc: FunctionCall => fc.function.isAggregate || fc.parameters.map(isAggregateExpression(_)).reduce(_ || _)
      case _ => false
    }
  }


  /**
    * Looks at the rollup columns supplied and tries to find one of the supplied functions whose first parameter
    * operates on the given ColumnRef.  If there are multiple matches, returns the first matching function.
    * Every function supplied must take at least one parameter.
    */
  private def findFunctionOnColumn(rollupColIdx: Map[Expr, Int],
                                   functions: Seq[Function[_]],
                                   colRef: ColumnRef): Option[Int] = {
    val possibleColIdxs = functions.map { function =>
      rollupColIdx.find {
        case (fc: FunctionCall, _) if fc.function.function == function && fc.parameters.head == colRef => true
        case _ => false
      }.map(_._2)
    }
    possibleColIdxs.flatten.headOption
  }

  /** An in order hierarchy of floating timestamp date truncation functions, from least granular to most granular. */
  private val dateTruncHierarchy = Seq(
    FloatingTimeStampTruncY,
    FloatingTimeStampTruncYm,
    FloatingTimeStampTruncYmd)

  /**
    * This tries to rewrite a between expression on floating timestamps to use an aggregated rollup column.
    * These functions have a hierarchy, so a query for a given level of truncation can be served by any other
    * truncation that is at least as long, eg. ymd can answer ym queries.
    *
    * This is slightly different than the more general expression rewrites because BETWEEN returns a boolean, so
    * there is no need to apply the date aggregation function on the ColumnRef.  In fact, we do not want to
    * apply the aggregation function on the ColumnRef because that will end up being bad for query execution
    * performance in most cases.
    *
    * @param fc A NOT BETWEEN or BETWEEN function call.
    */
  private def rewriteDateTruncBetween(r: Analysis, rollupColIdx: Map[Expr, Int], fc: FunctionCall): Option[Expr] = {
    assert(fc.function.function == Between || fc.function.function == NotBetween)
    val maybeColRef +: lower +: upper +: _ = fc.parameters
    val commonTruncFunction = commonDateTrunc(lower, upper)

    /** The column index in the rollup that matches the common truncation function,
      * either exactly or at a more granular level */
    val colIdx = maybeColRef match {
      case colRef: ColumnRef if colRef.typ == SoQLFloatingTimestamp =>
        for {
          // we can rewrite to any date_trunc_xx that is the same or after the desired one in the hierarchy
          possibleTruncFunctions <- commonTruncFunction.map { tf => dateTruncHierarchy.dropWhile { f => f != tf } }
          idx <- findFunctionOnColumn(rollupColIdx, possibleTruncFunctions, colRef)
        } yield idx
      case _ => None
    }

    /** Now rewrite the BETWEEN to use the rollup, if possible. */
    colIdx match {
      case Some(idx: Int) =>
        // All we have to do is replace the first argument with the rollup column reference since it is just
        // being used as a comparison that result in a boolean, then rewrite the b and c expressions.
        // ie. 'foo_date BETWEEN date_trunc_y("2014/01/01") AND date_trunc_y("2019/05/05")'
        // just has to replace foo_date with rollup column "c<n>"
        for {
          lowerRewrite <- apply(lower, r, rollupColIdx)
          upperRewrite <- apply(upper, r, rollupColIdx)
          newParams <- Some(Seq(
            typed.ColumnRef(NoQualifier, rollupColumnId(idx), SoQLFloatingTimestamp.t)(fc.position),
            lowerRewrite,
            upperRewrite))
        } yield fc.copy(parameters = newParams)
      case _ => None
    }
  }

  /**
    * Rewrite "less than" and "greater to or equal" to use rollup columns.  Note that date_trunc_xxx functions
    * are a form of a floor function.  This means that date_trunc_xxx(column) >= value will always be
    * the same as column >= value iff value could have been output by date_trunc_xxx.  Similar holds
    * for Lt, only it has to be strictly less since floor rounds down.
    *
    * For example, column >= '2014-03-01' AND column < '2015-05-01' can be rewritten as
    * date_trunc_ym(column) >= '2014-03-01' AND date_trunc_ym(column) < '2015-05-01' without changing
    * the results.
    *
    * Note that while we wouldn't need any of the logic here if queries explicitly came in as filtering
    * on date_trunc_xxx(column), we do not want to encourage that form of querying since is typically
    * much more expensive when you can't hit a rollup table.
    */
  private def rewriteDateTruncGteLt(r: Analysis, rollupColIdx: Map[Expr, Int], fc: FunctionCall): Option[Expr] = {
    fc.function.function match {
      case Lt | Gte =>
        val left +: right +: _ = fc.parameters
        (left, right) match {
          // The left hand side should be a floating timestamp, and the right hand side will be a string being cast
          // to a floating timestamp.  eg. my_floating_timestamp < '2010-01-01'::floating_timestamp
          // While it is eminently reasonable to also accept them in flipped order, that is being left for later.
          case (colRef@typed.ColumnRef(_, _, SoQLFloatingTimestamp),
          cast@typed.FunctionCall(MonomorphicFunction(TextToFloatingTimestamp, _), Seq(typed.StringLiteral(ts, _)), None, None)) =>
            for {
              parsedTs <- SoQLFloatingTimestamp.StringRep.unapply(ts)
              truncatedTo <- truncatedTo(SoQLFloatingTimestamp(parsedTs))
              // we can rewrite to any date_trunc_xx that is the same or after the desired one in the hierarchy
              possibleTruncFunctions <- Some(dateTruncHierarchy.dropWhile { f => f != truncatedTo })
              rollupColIdx <- findFunctionOnColumn(rollupColIdx, possibleTruncFunctions, colRef)
              newParams <- Some(Seq(typed.ColumnRef(
                NoQualifier,
                rollupColumnId(rollupColIdx),
                SoQLFloatingTimestamp.t)(fc.position), right))
            } yield fc.copy(parameters = newParams)
          case _ => None
        }
      case _ => None
    }
  }

  private def rewriteDateTrunc(r: Analysis, rollupColIdx: Map[Expr, Int], fc: FunctionCall): Option[Expr] = {
    val fn = fc.function.function
    val dateTrunFn: Option[Function[SoQLType]] = dateTruncHierarchy.find(f => f == fn)
    dateTrunFn.flatMap { _ =>
      rollupColIdx.get(fc) match {
        case Some(ruColIdx) =>
          Some(typed.ColumnRef(NoQualifier, rollupColumnId(ruColIdx), SoQLFloatingTimestamp.t)(fc.position))
        case None =>
          val left = fc.parameters.headOption
          left match {
            // The left hand side should be a floating timestamp, and the right hand side will be a string being cast
            // to a floating timestamp.  eg. my_floating_timestamp < '2010-01-01'::floating_timestamp
            // While it is eminently reasonable to also accept them in flipped order, that is being left for later.
            case Some(colRef@typed.ColumnRef(_, _, SoQLFloatingTimestamp)) =>
              for {
                truncatedTo <- Some(fc.function.function)
                // we can rewrite to any date_trunc_xx that is the same or after the desired one in the hierarchy
                possibleTruncFunctions <- Some(dateTruncHierarchy.dropWhile { f => f != truncatedTo })
                // match date_trunc hierachy or plain datetime column without truncate
                ruColIdx <- findFunctionOnColumn(rollupColIdx, possibleTruncFunctions, colRef).orElse(rollupColIdx.get(colRef))
              } yield {
                val newParams = Seq(typed.ColumnRef(NoQualifier, rollupColumnId(ruColIdx), SoQLFloatingTimestamp.t)(fc.position))
                fc.copy(parameters = newParams)
              }
            case Some(l) if isConstant(l) =>
              Some(fc)
            case _ =>
              None
          }
      }
    }
  }

  private def rewriteParameters(r: Analysis, rollupColIdx: Map[Expr, Int],
                                fc: FunctionCall): Option[typed.CoreExpr[ColumnId, SoQLType] with Serializable] = {
    val mapped = fc.parameters.map(fe => apply(fe, r, rollupColIdx))
    log.trace("mapped expr params {} {} -> {}", "", fc.parameters, mapped)
    if (mapped.forall(fe => fe.isDefined)) {
      log.trace("expr params all defined")
      Some(fc.copy(parameters = mapped.flatten))
    } else {
      None
    }
  }

}


object ExpressionRewriter {

  import com.socrata.soql.typed._ // scalastyle:ignore import.grouping

  def simplifyAndTrue(optExpr: Option[Expr]): Option[Expr] = {
    optExpr match {
      case Some(BooleanLiteral(true, _)) =>
        None
      case Some(FunctionCall(fn, Seq(left, right), _, _)) if fn.name == And.name =>
        (simplifyAndTrue(Some(left)), simplifyAndTrue(Some(right))) match {
          case (None, None) => None
          case (l, None) => l
          case (None, r) => r
          case _ => optExpr
        }
      case _ =>
        optExpr
    }
  }

}