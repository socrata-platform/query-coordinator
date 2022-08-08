package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryRewriter._
import com.socrata.soql.ast.{JoinFunc, JoinQuery, JoinTable, Select, StarSelection}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.functions._
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.typed.{ColumnRef => _, FunctionCall => _, Join => _, OrderBy => _, _}
import com.socrata.soql.types._
import com.socrata.soql.{AnalysisContext, BinaryTree, Compound, Leaf, ParameterSpec, PipeQuery, SoQLAnalysis, SoQLAnalyzer, typed}
import org.joda.time.{DateTimeConstants, LocalDateTime}

import scala.util.parsing.input.NoPosition
import scala.util.{Failure, Success, Try}

class QueryRewriter(analyzer: SoQLAnalyzer[SoQLType, SoQLValue]) extends CompoundQueryRewriter {

  import com.socrata.querycoordinator.util.Join._

  val log = org.slf4j.LoggerFactory.getLogger(classOf[QueryRewriter])

  private def ensure(expr: Boolean, msg: String): Option[String] = if (!expr) Some(msg) else None

  // TODO the secondary should probably just give us the names of the columns when we ask about the rollups
  // instead of assuming.
  private[querycoordinator] def rollupColumnId(idx: Int): String = "c" + (idx + 1)

  /** Maps the rollup column expression to the 0 based index in the rollup table.  If we have
    * multiple columns with the same definition, that is fine but we will only use one.
    */
  def rewriteSelection(q: Selection, r: Anal, rollupColIdx: Map[Expr, Int]): Option[Selection] = {
    val mapped: OrderedMap[ColumnName, Option[Expr]] = q.mapValues(e => rewriteExpr(e, r, rollupColIdx))

    if (mapped.values.forall(c => c.isDefined)) {
      Some(mapped.mapValues { v => v.get })
    } else {
      None
    }
  }

  private def removeAggregates(opt: OrderBy): OrderBy = {
    opt.copy(expression = removeAggregates(opt.expression))
  }

  /**
    * Used to remove aggregate functions from the expression in the case where the rollup and query groupings match
    * exactly.
    */
  private def removeAggregates(e: Expr): Expr = {
    e match {
      // If we have reached this point in the rewrite and have mapped things, I think all aggregate functions can
      // only have one argument.  We don't have any multi argument ones right now though so that is hard to test.
      // Given that, falling back to not rewriting and probably generating a query that fails to parse and errors
      // out seems better than throwing away other arguments and returning wrong results if we do need to consider them.
      case fc: FunctionCall if fc.function.isAggregate && fc.parameters.size == 1 =>
        fc.parameters.head
      case fc: FunctionCall =>
        // recurse in case we have a function on top of an aggregation
        fc.copy(parameters = fc.parameters.map(p => removeAggregates(p)))
      case _ => e
    }
  }

  def rewriteJoin(joins: Seq[Join], r: Anal): Option[Seq[Join]] = {
    if (joins == r.joins) {
      Some(Seq.empty)
    } else {
      None
    }
  }

  def rewriteGroupBy(qOpt: GroupBy, r: Anal, qSelection: Selection,
      rollupColIdx: Map[Expr, Int]): Option[GroupBy] = {
    val rOpt: GroupBy = r.groupBys
    (qOpt, rOpt) match {
      // If the query has no group by and the rollup has no group by then all is well
      case (Nil, Nil) => Some(Nil)
      // If the query and the rollup are grouping on the same columns (possibly in a different order) then
      // we can just leave the grouping off when querying the rollup to make it less expensive.
      case (q, r) if q.nonEmpty && r.nonEmpty && q.toSet == r.toSet => Some(Nil)
      // If the query isn't grouped but the rollup is, everything in the selection must be an aggregate.
      // For example, a "SELECT sum(cost) where type='Boat'" could be satisfied by a rollup grouped by type.
      // We rely on the selection rewrite to ensure the columns are there, validate if it is self aggregatable, etc.
      case (Nil, r) if r.nonEmpty && qSelection.forall { case (_, e) => selectableWhenGroup(e) } => Some(Nil)
      // if the query is grouping, every grouping in the query must grouped in the rollup.
      // The analysis already validated there are no un-grouped columns in the selection
      // that aren't in the group by.
      case (q, _) if q.nonEmpty =>
        val grouped = q.map { expr => rewriteExpr(expr, r, rollupColIdx) }

        if (grouped.forall(_.isDefined)) {
          Some(grouped.flatten)
        } else {
          None
        }
      // TODO We can also rewrite if the rollup has no group bys, but need to do the right aggregate
      // manipulation.
      case _ => None
    }
  }

  /** Find the first column index that is a count(*) or count(literal). */
  def findCountStarOrLiteral(rollupColIdx: Map[Expr, Int]): Option[Int] = {
    rollupColIdx.find {
      case (fc: FunctionCall, _) => isCountStarOrLiteral(fc)
      case _ => false
    }.map(_._2)
  }

  /** Is the given function call a count(*) or count(literal), excluding NULL because it is special.  */
  private def isCountStarOrLiteral(fc: FunctionCall): Boolean = {
    fc.function.function == CountStar ||
      (fc.function.function == Count &&
        fc.parameters.forall {
          case e: typed.NullLiteral[_] => false
          case e: typed.TypedLiteral[_] => true
          case _ => false
        })
  }

  /** Can this function be applied to its own output in a further aggregation */
  private def isSelfAggregatableAggregate(f: Function[_]): Boolean = f match {
    case Max | Min | Sum => true
    case _ => false
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

  /** An in order hierarchy of floating timestamp date truncation functions, from least granular to most granular.  */
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
  private def rewriteDateTruncBetweenExpr(r: Anal, rollupColIdx: Map[Expr, Int], fc: FunctionCall): Option[Expr] = {
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
          lowerRewrite <- rewriteExpr(lower, r, rollupColIdx)
          upperRewrite <- rewriteExpr(upper, r, rollupColIdx)
          newParams <- Some(Seq(
            typed.ColumnRef(NoQualifier, rollupColumnId(idx), SoQLFloatingTimestamp.t)(fc.position),
            lowerRewrite,
            upperRewrite))
        } yield fc.copy(parameters = newParams)
      case _ => None
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

  /**
   * Returns the least granular date truncation function that can be applied to the timestamp
   * without changing its value.
   */
  private[querycoordinator] def truncatedTo(soqlTs: SoQLFloatingTimestamp)
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
  private def rewriteDateTruncGteLt(r: Anal, rollupColIdx: Map[Expr, Int], fc: FunctionCall): Option[Expr] = {
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

  private def rewriteDateTrunc(r: Anal, rollupColIdx: Map[Expr, Int], fc: FunctionCall): Option[Expr] = {
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

  /** Recursively maps the Expr based on the rollupColIdx map, returning either
    * a mapped expression or None if the expression couldn't be mapped.
    *
    * Note that every case here needs to ensure to map every expression recursively
    * to ensure it is either a literal or mapped to the rollup.
    */
  def rewriteExpr(e: Expr, r: Anal, rollupColIdx: Map[Expr, Int]): Option[Expr] = { // scalastyle:ignore cyclomatic.complexity
    log.trace("Attempting to match expr: {}", e)
    e match {
      case literal: typed.TypedLiteral[_] => Some(literal) // This is literally a literal, so so literal.
      // for a column reference we just need to map the column id
      case cr: ColumnRef => for {idx <- rollupColIdx.get(cr)} yield cr.copy(qualifier = None, column = rollupColumnId(idx))
      // window functions run after where / group by / having so can't be rewritten in isolation.  We could
      // still rewrite a query with a window function in if the other clauses all match, however that requires
      // coordination between expression mapping and mapping other clauses at a higher level that isn't implemented,
      // so for now we just forbid it entirely to avoid incorrect rewrites.
      case fc: FunctionCall if fc.window.nonEmpty =>
        None
      // A count(*) or count(non-null-literal) on q gets mapped to a sum on any such column in rollup
      case fc: FunctionCall if isCountStarOrLiteral(fc) =>
        val filterAndRollupWhere = andRollupWhereToFilter(fc.filter, r: Anal)
        for {
          idx <- findCountStarOrLiteral(rollupColIdx) // find count(*) column in rollup
          rwFilter <- rewriteWhere(filterAndRollupWhere, r, rollupColIdx)
        } yield {
          val simplifiedRwFilter = simplifyAndTrue(rwFilter)
          val newSumCol = typed.ColumnRef(NoQualifier, rollupColumnId(idx), SoQLNumber.t)(fc.position)
          typed.FunctionCall(CoalesceNumber, Seq(typed.FunctionCall(SumNumber, Seq(newSumCol), simplifiedRwFilter, fc.window)(fc.position, fc.position),
                                                 typed.NumberLiteral(0, SoQLNumber.t)(fc.position)), None, fc.window)(fc.position, fc.position)
        }
      // A count(...) on q gets mapped to a sum(...) on a matching column in the rollup.  We still need the count(*)
      // case above to ensure we can do things like map count(1) and count(*) which aren't exact matches.
      case fc@typed.FunctionCall(MonomorphicFunction(Count, _), _, filter, window) =>
        val fcMinusFilter = stripFilter(fc)
        val filterAndRollupWhere = andRollupWhereToFilter(filter, r: Anal)
        for {
          idx <- rollupColIdx.get(fcMinusFilter) // find count(...) in rollup that matches exactly
          rwFilter <- rewriteWhere(filterAndRollupWhere, r, rollupColIdx)
        } yield {
          val simplifiedRwFilter = simplifyAndTrue(rwFilter)
          val newSumCol = typed.ColumnRef(NoQualifier, rollupColumnId(idx), SoQLNumber.t)(fc.position)
          typed.FunctionCall(CoalesceNumber, Seq(typed.FunctionCall(SumNumber, Seq(newSumCol), simplifiedRwFilter, window)(fc.position, fc.position),
                                                 typed.NumberLiteral(0, SoQLNumber.t)(fc.position)), None, window)(fc.position, fc.position)
        }
      // An avg(...) on q gets mapped to a sum(...)/count(...) on a matching column in the rollup
      case fc@typed.FunctionCall(MonomorphicFunction(Avg, _), _, filter, window) =>
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
      // If this is a between function operating on floating timestamps, and arguments b and c are both date aggregates,
      // then try to rewrite argument a to use a rollup.
      case fc: FunctionCall
        if (fc.function.function == Between || fc.function.function == NotBetween) &&
          fc.function.bindings.values.forall(_ == SoQLFloatingTimestamp) &&
          fc.function.bindings.values.tail.forall(dateTruncHierarchy contains _) =>
        rewriteDateTruncBetweenExpr(r, rollupColIdx, fc)
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
      case fc: FunctionCall if !fc.function.isAggregate => rewriteNonagg(r, rollupColIdx, fc)
      // If the function is "self aggregatable" we can apply it on top of an already aggregated rollup
      // column, eg. select foo, bar, max(x) max_x group by foo, bar --> select foo, max(max_x) group by foo
      // If we have a matching column, we just need to update its argument to reference the rollup column.
      case fc: FunctionCall if isSelfAggregatableAggregate(fc.function.function) =>
        val fcMinusFilter = stripFilter(fc)
        val filterAndRollupWhere = andRollupWhereToFilter(fc.filter, r: Anal)
        for {
          idx <- rollupColIdx.get(fcMinusFilter)
          rwFilter <- rewriteWhere(filterAndRollupWhere, r, rollupColIdx)
        } yield {
          val simplifiedRwFilter = simplifyAndTrue(rwFilter)
          fc.copy(parameters = Seq(typed.ColumnRef(NoQualifier, rollupColumnId(idx), fc.typ)(fc.position)),
                  filter = simplifiedRwFilter)
        }
      case _ => None
    }
  }

  // remaining non-aggregate functions
  private def rewriteNonagg(r: Anal, rollupColIdx: Map[Expr, Int],
                            fc: FunctionCall): Option[typed.CoreExpr[ColumnId, SoQLType] with Serializable] = {
    // if we have the exact same function in rollup and query, just turn it into a column ref in the rollup
    val functionMatch = for {
      idx <- rollupColIdx.get(fc)
    } yield typed.ColumnRef(NoQualifier, rollupColumnId(idx), fc.typ)(fc.position)
    // otherwise, see if we can recursively rewrite
    functionMatch.orElse {
      val mapped = fc.parameters.map(fe => rewriteExpr(fe, r, rollupColIdx))
      log.trace("mapped expr params {} {} -> {}", "", fc.parameters, mapped)
      if (mapped.forall(fe => fe.isDefined)) {
        log.trace("expr params all defined")
        Some(fc.copy(parameters = mapped.flatten))
      } else {
        None
      }
    }
  }

  def rewriteWhere(qeOpt: Option[Expr], r: Anal, rollupColIdx: Map[Expr, Int]): Option[Where] = {
    log.debug(s"Attempting to map query where expression '${qeOpt}' to rollup ${r}")

    val rw = (qeOpt, r.where) match {
      // To allow rollups with where clauses, validate that r.where is contained in q.where (top level and).
      case (Some(qe), Some(re)) if (topLevelAndContain(re, qe)) =>
        val strippedQe = stripExprInTopLevelAnd(re, qe)
        rewriteExpr(strippedQe, r, rollupColIdx).map(Some(_))
      case (_, Some(_)) =>
        None
      // no where on query or rollup, so good!  No work to do.
      case (None, None) => Some(None)
      // have a where on query so try to map recursively
      case (Some(qe), None) => rewriteExpr(qe, r, rollupColIdx).map(Some(_))
    }
    rw.map(simplifyAndTrue)
  }

  /**
    * @param qeOpt query having expression option
    * @param qbs group by sequence
    * @param r rollup analysis
    */
  def rewriteHaving(qeOpt: Option[Expr], qbs: Seq[Expr], r: Anal, rollupColIdx: Map[Expr, Int]): Option[Having] = {
    log.debug(s"Attempting to map query having expression '${qeOpt}' to rollup ${r}")

    val rw = (qeOpt, r.having) match {
      // To allow rollups with having clauses, validate that r.having is contained in q.having (top level and) and
      // they have the same group by.
      case (Some(qe), Some(re)) if (topLevelAndContain(re, qe) && qbs.toSet == r.groupBys.toSet) =>
        val strippedQe = stripExprInTopLevelAnd(re, qe)
        rewriteExpr(strippedQe, r, rollupColIdx).map(Some(_))
      case (_, Some(_)) =>
        None
      // no having on query or rollup, so good!  No work to do.
      case (None, None) => Some(None)
      // have a having on query so try to map recursively
      case (Some(qe), None) => rewriteExpr(qe, r, rollupColIdx).map(Some(_))
    }
    rw.map(simplifyAndTrue)
  }

  def rewriteOrderBy(obsOpt: Seq[OrderBy], r: Anal, rollupColIdx: Map[Expr, Int]): Option[Seq[OrderBy]] = {
    log.debug(s"Attempting to map order by expression '${obsOpt}'") // scalastyle:ignore multiple.string.literals

    // it is silly if the rollup has an order by, but we really don't care.
    obsOpt match {
      case Nil => Some(Nil)
      case obs =>
        val mapped = obs.map { ob =>
          rewriteExpr(ob.expression, r, rollupColIdx) match {
            case Some(e) => Some(ob.copy(expression = e))
            case None => None
          }
        }
        if (mapped.forall { ob => ob.nonEmpty }) {
          Some(mapped.flatten)
        } else {
          None
        }
    }
  }

  def possibleRewrites(q: Anal, rollups: Map[RollupName, Anal]): Map[RollupName, Anal] = {
    possibleRewrites(q, rollups, false)
  }

  def possibleRewrites(q: Anal, rollups: Map[RollupName, Anal], debug: Boolean): Map[RollupName, Anal] = {
    log.debug("looking for candidates to rewrite for query: {}", q)

    if (noRollup(q)) {
      return Map.empty
    }

    val candidates = rollups.map { case (name, r) =>
      log.debug("checking for compat with: {}", r)

      // this lets us lookup the column and get the 0 based index in the select list
      val rollupColIdx = r.selection.values.zipWithIndex.toMap

      val groupBy = rewriteGroupBy(q.groupBys, r, q.selection, rollupColIdx)
      val where = rewriteWhere(q.where, r, rollupColIdx)

      /*
       * As an optimization for query performance, the group rewrite code can turn a grouped query into an ungrouped
       * query if the grouping matches.  If it did that, we need to fix up the selection and ordering (and having if it
       * we supported it)  We need to ensure we don't remove aggregates from a query without any group bys to
       * start with, eg. "SELECT count(*)".  The rewriteGroupBy call above will indicate we can do this by returning
       * a Some(None) for the group bys.
       *
       * For example:
       * rollup: SELECT crime_type AS c1, count(*) AS c2, max(severity) AS c3 GROUP BY crime_type
       * query: SELECT crime_type, count(*), max(severity) GROUP BY crime_type
       * previous rollup query: SELECT c1, sum(c2), max(c3) GROUP BY c1
       * desired rollup query:  SELECT c1, c2, c3
       */
      val shouldRemoveAggregates = groupBy match {
        case Some(Nil) => q.groupBys.nonEmpty
        case _ => false
      }

      val selection = rewriteSelection(q.selection, r, rollupColIdx) map { s =>
        if (shouldRemoveAggregates) s.mapValues(removeAggregates)
        else s
      }

      val orderBy = rewriteOrderBy(q.orderBys, r, rollupColIdx) map { o =>
        if (shouldRemoveAggregates) o.map(removeAggregates)
        else o
      }

      val having = rewriteHaving(q.having, q.groupBys, r, rollupColIdx) map { h =>
        if (shouldRemoveAggregates) h.map(removeAggregates)
        else h
      }

      val joins = rewriteJoin(q.joins, r)

      val mismatch =
        ensure(selection.isDefined, "mismatch on select") orElse
          ensure(where.isDefined, "mismatch on where") orElse
          ensure(groupBy.nonEmpty, "mismatch on groupBy") orElse
          ensure(having.isDefined, "mismatch on having") orElse
          ensure(orderBy.nonEmpty, "mismatch on orderBy") orElse
          ensure(joins.nonEmpty,"mismatch on join") orElse
          // For limit and offset, we can always apply them from the query  as long as the rollup
          // doesn't have any.  For certain cases it would be possible to rewrite even if the rollup
          // has a limit or offset, but we currently don't.
          ensure(None == r.limit, "mismatch on limit") orElse
          ensure(None == r.offset, "mismatch on offset") orElse
          ensure(q.search == None, "mismatch on search") orElse
          ensure(q.distinct == r.distinct, "mismatch on distinct")

      mismatch match {
        case None =>
          if (debug) {
            log.info(s"rollup matched ${name}")
          }
          (name, Some(r.copy(
            isGrouped = if (shouldRemoveAggregates) groupBy.exists(_.nonEmpty) else q.isGrouped,
            selection = selection.get,
            joins = joins.get,
            groupBys = groupBy.get,
            orderBys = orderBy.get,
            // If we are removing aggregates then we are no longer grouping and need to put the condition
            // in the where instead of the having.
            where = if (shouldRemoveAggregates) andExpr(where.get, having.get) else where.get,
            having = if (shouldRemoveAggregates) None else having.get,
            limit = q.limit,
            offset = q.offset
          )))
        case Some(s) =>
          log.debug("Not compatible: {}", s)
          if (debug) {
            log.info(s"rollup not matched ${name} $s")
          }
          (name, None)
      }
    }

    log.debug("Final candidates: {}", candidates)
    candidates.collect { case (k, Some(v)) => k -> v }
  }

  def andExpr(a: Option[Expr], b: Option[Expr]): Option[Expr] = {
    (a, b) match {
      case (None, None) => None
      case (Some(a), None) => Some(a)
      case (None, Some(b)) => Some(b)
      case (Some(a), Some(b)) => Some(typed.FunctionCall(SoQLFunctions.And.monomorphic.get, List(a, b), None, None)(NoPosition, NoPosition))
    }
  }


  /**
   * For analyzing the rollup query, we need to map the dataset schema column ids to the "_" prefixed
   * version of the name that we get, designed to ensure the column name is valid soql
   * and doesn't start with a number.
   */
  private def addRollupPrefix(name: ColumnId): ColumnName = {
    new ColumnName(if (name(0) == ':') name else "_" + name)
  }


  /**
   * Once we have the analyzed rollup query with column names, we need to remove the leading "_" on non-system
   * columns to make the names match up with the underlying dataset schema.
   */
  private def removeRollupPrefix(cn: ColumnName, qual: Qualifier): ColumnId = { // TODO: Join
    cn.name(0) match {
      case '_' => cn.name.drop(1)
      case _ => cn.name // leave system columns (prefixed ':') or derived columns unchanged
    }
  }

  // maps prefixed column name to type
  private def prefixedDsContext(schema: Schema) = {
    val columnIdMap = schema.schema.map { case (k, v) => addRollupPrefix(k) -> k }
    QueryParser.dsContext(columnIdMap, schema.schema)
  }

  def analyzeRollups(schema: Schema, rollups: Seq[RollupInfo],
                     getSchemaByTableName: TableName => SchemaWithFieldName): Map[RollupName, BinaryTree[Anal]] = {

    val dsContext = Map(TableName.PrimaryTable.qualifier -> prefixedDsContext(schema))
    val rollupMap = rollups.map { r => (r.name, r.soql) }.toMap
    val analysisMap = rollupMap.mapValues { soql =>
      Try {
        val parsedQueries = new StandaloneParser().binaryTreeSelect(soql)
        val tableNames = collectTableNames(parsedQueries)
        val schemas = tableNames.foldLeft(dsContext) { (acc, tn) =>
          val tableName = TableName(tn)
          val sch = getSchemaByTableName(tableName)
          val dsctx = prefixedDsContext(sch.toSchema())
          acc + (tn -> dsctx)
        }
        val context = AnalysisContext[SoQLType, SoQLValue](
          schemas,
          parameters = ParameterSpec.empty
        )

        val analyses = analyzer.analyzeBinary(parsedQueries)(context) map { analysis =>
          analysis.mapColumnIds(removeRollupPrefix)
        }
        analyses
      }
    }

    analysisMap.foreach {
      case (rollupName, Failure(e: SoQLException)) => log.info(s"Couldn't parse rollup $rollupName, ignoring: ${e.toString}")
      case (rollupName, Failure(e)) => log.warn(s"Couldn't parse rollup $rollupName due to unexpected failure, ignoring", e)
      case _ =>
    }

    analysisMap collect {
      case (k, Success(ruAnalysis)) =>
        k -> ruAnalysis
    }
  }
}


object QueryRewriter {
  import com.socrata.soql.typed._ // scalastyle:ignore import.grouping

  type Anal = SoQLAnalysis[ColumnId, SoQLType]
  type ColumnId = String
  type ColumnRef = typed.ColumnRef[ColumnId, SoQLType]
  type Expr = CoreExpr[ColumnId, SoQLType]
  type FunctionCall = typed.FunctionCall[ColumnId, SoQLType]
  type GroupBy = Seq[Expr]
  type OrderBy = typed.OrderBy[ColumnId, SoQLType]
  type RollupName = String
  type Selection = OrderedMap[ColumnName, Expr]
  type Where = Option[Expr]
  type Having = Option[Expr]
  type Join = typed.Join[ColumnId, SoQLType]

  val SumNumber = MonomorphicFunction(Sum, Map("a" -> SoQLNumber))
  val CoalesceNumber = MonomorphicFunction(Coalesce, Map("a" -> SoQLNumber))
  val DivNumber = DivNumNum.monomorphic.get
  val CountNumber = MonomorphicFunction(Count, Map("a" -> SoQLNumber))

  private def noRollup(q: Anal): Boolean = {
    q.hints.exists {
      case NoRollup(_) =>
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

  private def topLevelAndContain(target: Expr, expr: Expr): Boolean = {
    expr match {
      case e: Expr if e == target =>
        true
      case fc: FunctionCall if fc.function.name == And.name =>
        topLevelAndContain(target, fc.parameters.head) ||
          topLevelAndContain(target, fc.parameters.tail.head)
      case _ =>
        false
    }
  }

  private def stripExprInTopLevelAnd(target: Expr, expr: Expr): Expr = {
    expr match {
      case e: Expr if e == target =>
        typed.BooleanLiteral(true, SoQLBoolean.t)(e.position)
      case fc: FunctionCall if fc.function.name == And.name =>
        fc.copy(parameters = fc.parameters.map(stripExprInTopLevelAnd(target, _)))
      case e =>
        e
    }
  }

  private def andRollupWhereToFilter(filter: Option[Expr], r: Anal): Option[Expr] = {
    (r.where, filter) match {
      case (None, _) => filter
      case (s@Some(_), None) => s
      case (Some(rw), Some(f)) =>
        Some(typed.FunctionCall(SoQLFunctions.And.monomorphic.get, List(rw, f), None, None)(rw.position, rw.position))
    }
  }

  private def simplifyAndTrue(optExpr: Option[Expr]): Option[Expr] = {
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

  /**
    * Whether the expression is valid in SELECT clause when the query is grouped (both explicitly or implicitly)
    */
  private def selectableWhenGroup(expr: Expr): Boolean = {
    expr match {
      case fc: FunctionCall if fc.function.isAggregate =>
        true
      case fc: FunctionCall =>
        fc.parameters.forall(selectableWhenGroup)
      case _: typed.TypedLiteral[_] =>
        true
      case _ =>
        false
    }
  }

  def collectTableNames(selects: BinaryTree[Select]): Set[String] = {
    selects match {
      case Compound(_, l, r) =>
        collectTableNames(l) ++ collectTableNames(r)
      case Leaf(select) =>
        select.joins.foldLeft(select.from.map(_.name).filter(_ != TableName.This).toSet) { (acc, join) =>
          join.from match {
            case JoinTable(TableName(name, _)) =>
              acc + name
            case JoinQuery(selects, _) =>
              acc ++ collectTableNames(selects)
            case JoinFunc(_, _) =>
              throw new Exception("Unsupported join function")
          }
        }
    }
  }

  /**
    * Filter rollups to non-compound query, i.e. leaf
    */
  def simpleRollups(rus: Map[RollupName, BinaryTree[Anal]]): Map[RollupName, Anal] = {
    rus.filter {
      case (_, Leaf(_)) => true
      case _ => false
    }.mapValues(_.outputSchema.leaf)
  }
}
