package com.socrata.querycoordinator.rollups

import com.socrata.querycoordinator._
import com.socrata.querycoordinator.rollups.ExpressionRewriter._
import com.socrata.querycoordinator.rollups.QueryRewriter._
import com.socrata.querycoordinator.rollups.BaseQueryRewriter._
import com.socrata.soql._
import com.socrata.soql.ast.{JoinFunc, JoinQuery, JoinTable, Select}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.functions._
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.typed.{ColumnRef => _, FunctionCall => _, Join => _, OrderBy => _, _}
import com.socrata.soql.types._

import scala.util.parsing.input.NoPosition
import scala.util.{Failure, Success, Try}

abstract class BaseQueryRewriter(analyzer: SoQLAnalyzer[SoQLType, SoQLValue]) {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[BaseQueryRewriter])

  private def ensure(expr: Boolean, msg: String): Option[String] = if (!expr) Some(msg) else None

  // TODO the secondary should probably just give us the names of the columns when we ask about the rollups
  // instead of assuming.
  private[querycoordinator] def rollupColumnId(idx: Int): String = "c" + (idx + 1)

  private[querycoordinator] val rewriteExpr = new ExpressionRewriter(rollupColumnId, rewriteWhere)
  private val rewriteExprWithClauses = new ClausesMatchedExpressionRewriter(rollupColumnId, rewriteWhere)

  def possiblyRewriteQuery(dataset: String,
                                   analyzedQuery: SoQLAnalysis[String, SoQLType],
                                   rollups: Map[RollupName, Analysis],
                                   debug: Boolean = false):
  (SoQLAnalysis[String, SoQLType], Option[String]) = {

    val rewritten = bestRollup(possibleRewrites(analyzedQuery, rollups, debug).toSeq)

    val (rollupName, analysis) = rewritten.map {
      case (rollupName, analysis) => (Option(rollupName), analysis)
    }.getOrElse((None, analyzedQuery))

    rollupName.foreach(ru => log.info(s"Rewrote query on dataset $dataset to rollup $ru")) // only log rollup name if it is defined.
    log.debug(s"Rewritten analysis: $analysis")
    (analysis, rollupName)
  }


  /** Maps the rollup column expression to the 0 based index in the rollup table.  If we have
    * multiple columns with the same definition, that is fine but we will only use one.
    */
  private def rewriteSelection(q: Selection, r: Analysis, rollupColIdx: Map[Expr, Int], allClausesMatch:Boolean = false): Option[Selection] = {
    val expressionRewriter:ExpressionRewriter = if(allClausesMatch){
      //Maps window functions
      rewriteExprWithClauses
    }else {
      //Does not map window functions
      rewriteExpr
    }
    val mapped: OrderedMap[ColumnName, Option[Expr]] = q.mapValues(e => expressionRewriter(e, r, rollupColIdx))

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

  private def rewriteJoin(joins: Seq[Join], r: Analysis): Option[Seq[Join]] = {
    if (joins == r.joins) {
      Some(Seq.empty)
    } else {
      None
    }
  }

  private def rewriteGroupBy(qOpt: GroupBy, r: Analysis, qSelection: Selection,
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

  private def rewriteWhere(qeOpt: Option[Expr], r: Analysis, rollupColIdx: Map[Expr, Int]): Option[Where] = {
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
    * @param qbs   group by sequence
    * @param r     rollup analysis
    */
  private def rewriteHaving(qeOpt: Option[Expr], qbs: Seq[Expr], r: Analysis, rollupColIdx: Map[Expr, Int]): Option[Having] = {
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

  private def rewriteOrderBy(obsOpt: Seq[OrderBy], r: Analysis, rollupColIdx: Map[Expr, Int]): Option[Seq[OrderBy]] = {
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

  def possibleRewrites(q: Analysis, rollups: Map[RollupName, Analysis]): Map[RollupName, Analysis] = {
    possibleRewrites(q, rollups, false)
  }

  def possibleRewrites(q: Analysis, rollups: Map[RollupName, Analysis], debug: Boolean): Map[RollupName, Analysis] = {
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


      val orderBy = rewriteOrderBy(q.orderBys, r, rollupColIdx) map { o =>
        if (shouldRemoveAggregates) o.map(removeAggregates)
        else o
      }

      val having = rewriteHaving(q.having, q.groupBys, r, rollupColIdx) map { h =>
        if (shouldRemoveAggregates) h.map(removeAggregates)
        else h
      }

      val selection = rewriteSelection(q.selection, r, rollupColIdx,(r.where.equals(q.where)&&r.groupBys.equals(q.groupBys)&&r.having.equals(q.having))) map { s =>
        if (shouldRemoveAggregates) s.mapValues(removeAggregates)
        else s
      }

      val joins = rewriteJoin(q.joins, r)

      val mismatch =
        ensure(selection.isDefined, "mismatch on select") orElse
          ensure(where.isDefined, "mismatch on where") orElse
          ensure(groupBy.nonEmpty, "mismatch on groupBy") orElse
          ensure(having.isDefined, "mismatch on having") orElse
          ensure(orderBy.nonEmpty, "mismatch on orderBy") orElse
          ensure(joins.nonEmpty, "mismatch on join") orElse
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

  private def andExpr(a: Option[Expr], b: Option[Expr]): Option[Expr] = {
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

  private[querycoordinator] def analyzeRollups(schema: Schema, rollups: Seq[RollupInfo],
                     getSchemaByTableName: TableName => SchemaWithFieldName): Map[RollupName, AnalysisTree] = {

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

  private[querycoordinator] def bestRollup(rollups: Seq[(RollupName, Analysis)]): Option[(RollupName, Analysis)] = RollupScorer.bestRollup(rollups)
}


object BaseQueryRewriter {

  import com.socrata.soql.typed._ // scalastyle:ignore import.grouping

  type ColumnRef = typed.ColumnRef[ColumnId, SoQLType]
  type FunctionCall = typed.FunctionCall[ColumnId, SoQLType]
  type GroupBy = Seq[Expr]
  type OrderBy = typed.OrderBy[ColumnId, SoQLType]
  type Selection = OrderedMap[ColumnName, Expr]
  type Where = Option[Expr]
  type Having = Option[Expr]
  type Join = typed.Join[ColumnId, SoQLType]

  val SumNumber = MonomorphicFunction(Sum, Map("a" -> SoQLNumber))
  val CoalesceNumber = MonomorphicFunction(Coalesce, Map("a" -> SoQLNumber))
  val DivNumber = DivNumNum.monomorphic.get
  val CountNumber = MonomorphicFunction(Count, Map("a" -> SoQLNumber))


  def rollupAtJoin(q: Analysis): Boolean = {
    q.hints.exists {
      case RollupAtJoin(_) => true
      case _ => false
    }
  }

  def compoundRollup(q: Analysis): Boolean = {
    q.hints.exists {
      case CompoundRollup(_) => true
      case _ => false
    }
  }

  private def noRollup(q: Analysis): Boolean = {
    q.hints.exists {
      case NoRollup(_) =>
        true
      case _ =>
        false
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

  def primaryRollup(names: Seq[String]): Option[String] = {
    names.filterNot(_.startsWith(TableName.SoqlPrefix)).headOption
  }
}
