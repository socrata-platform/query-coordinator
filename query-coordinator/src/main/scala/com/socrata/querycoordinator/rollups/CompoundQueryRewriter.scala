package com.socrata.querycoordinator.rollups



import com.socrata.querycoordinator.rollups.QueryRewriter.{Analysis, AnalysisTree, ColumnId, RollupName}
import com.socrata.soql._
import com.socrata.soql.environment.{TableName}
import com.socrata.soql.typed.{ColumnRef, CompoundRollup, Indistinct}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery, SoQLAnalyzer}
import com.socrata.querycoordinator._


/**
  * Rewrite compound query with either exact tree match or prefix tree match
  * TODO: Make matching more flexible.  e.g. "SELECT a, b" does not match "SELECT b, a" or "SELECT a"
  */
class CompoundQueryRewriter(analyzer: SoQLAnalyzer[SoQLType, SoQLValue]) extends BaseQueryRewriter(analyzer) with QueryRewriter {


  def possiblyRewriteOneAnalysisInQuery(dataset: String,
                                        schema: Schema,
                                        analyzedQuery: BinaryTree[SoQLAnalysis[String, SoQLType]],
                                        ruMapOpt: Option[Map[RollupName, AnalysisTree]],
                                        rollupFetcher: () => Seq[RollupInfo],
                                        schemaFetcher: TableName => SchemaWithFieldName,
                                        debug: Boolean):
  (BinaryTree[SoQLAnalysis[String, SoQLType]], Seq[String]) = {

    val ruMap: Map[RollupName, AnalysisTree] = ruMapOpt.getOrElse(rollupFetcher() match {
      case Seq() => Map.empty
      case rollups => analyzeRollups(schema, rollups, schemaFetcher)
    })

    QueryRewriter.mergeAnalysis(analyzedQuery) match {
      case Compound(op, l, r) =>
        possiblyRewriteOneAnalysisInQuery(dataset, schema, l, Some(ruMap), rollupFetcher, schemaFetcher, debug) match {
          // Success? just return the rewrite
          case (rewrite, rollup) if rollup.nonEmpty => (Compound(op, rewrite, r), rollup)
          // Fail? return original query
          case _ => (analyzedQuery, Seq.empty)
        }

      case Leaf(analysis) =>
        val datasetOrResourceName = analysis.from match {
          case Some(TableName(TableName.This, _)) => Left(dataset)
          case Some(tableName@TableName(TableName.SingleRow, _)) => Right(tableName.name)
          case Some(tableName) => Right(tableName.name)
          case None => Left(dataset)
        }

        datasetOrResourceName match {
          case Left(dataset) =>
            val rus = QueryRewriter.mergeRollupsAnalysis(ruMap)
            possiblyRewriteQuery(dataset, analysis, rus, debug) match {
              case (rewrittenAnal, ru@Some(_)) =>
                (Leaf(rewrittenAnal), ru.toSeq)
              case (_, None) =>
                val (possiblyRewrittenAnalysis, rollupJoin) = possiblyRewriteJoin(analysis, rollupFetcher, schemaFetcher)
                (Leaf(possiblyRewrittenAnalysis), rollupJoin)
            }
          case Right(resourceName) =>
            // TODO: union cannot use RollUp yet.
            // Further work needs to decorate the analysis with more than one rollup info so that
            // soql-postgres-adapter can get to the rollup table name.
            // Options to put the additional rollups info -
            // 1. Request Header
            // 2. in Analysis.from
            (Leaf(analysis), Seq.empty)
        }
    }
  }

  def possiblyRewriteJoin(analyses: BinaryTree[SoQLAnalysis[String, SoQLType]],
                          rollupFetcher: () => Seq[RollupInfo],
                          schemaFetcher: TableName => SchemaWithFieldName):
  (BinaryTree[SoQLAnalysis[String, SoQLType]], Seq[String]) = {

    analyses match {
      case Compound(op, l, r) =>
        val (nl, rul) = possiblyRewriteJoin(l, rollupFetcher, schemaFetcher)
        val (nr, rur) = possiblyRewriteJoin(r, rollupFetcher, schemaFetcher)
        (Compound(op, nl, nr), rul ++ rur)
      case Leaf(l) =>
        val (nl, ru) = possiblyRewriteJoin(l, rollupFetcher, schemaFetcher)
        (Leaf(nl), ru)
    }
  }

  def possiblyRewriteJoin(analysis: SoQLAnalysis[String, SoQLType],
                          rollupFetcher: () => Seq[RollupInfo],
                          schemaFetcher: TableName => SchemaWithFieldName):
  (SoQLAnalysis[String, SoQLType], Seq[String]) = {

    if (!QueryRewriter.rollupAtJoin(analysis)) {
      return (analysis, Nil)
    }

    val (rwJoins, rus) = analysis.joins.foldLeft((Seq.empty[typed.Join[String, SoQLType]], Seq.empty[String])) { (acc, join) =>
      join.from.subAnalysis match {
        case Right(SubAnalysis(analyses, alias)) =>
          val leftMost = analyses.leftMost
          val leftMostFromRemoved = Leaf(leftMost.leaf.copy(from = None))
          val joinedAnalysesFromRemoved = analyses.replace(leftMost, leftMostFromRemoved)
          val joinedTable = leftMost.leaf.from.get

          rollupFetcher() match {
            case Seq() =>
              (acc._1 :+ join, acc._2)
            case rollups =>
              val joinedSchema = schemaFetcher(joinedTable)
              val analyzedRollupsOfJoinTable = analyzeRollups(joinedSchema.toSchema(), rollups, schemaFetcher)
              possibleRewrites(joinedAnalysesFromRemoved, analyzedRollupsOfJoinTable, false) match {
                case (rwAnalyses, Seq(ruApplied)) =>
                  val ruTableName = TableName(s"${joinedTable.name}.${ruApplied}")
                  val rwAnalysesRollupTableApplied = rwAnalyses.leftMost.leaf.copy(from = Some(ruTableName))
                  val rwSubAnalysis = SubAnalysis(Leaf(rwAnalysesRollupTableApplied), alias)
                  val rwJoinJoinAnalysis: JoinAnalysis[ColumnId, SoQLType] = join.from.copy(subAnalysis = Right(rwSubAnalysis))
                  val rwJoin = join.copy(from = rwJoinJoinAnalysis)
                  (acc._1 :+ rwJoin, acc._2 :+ ruTableName.nameWithSoqlPrefix)
                case _ =>
                  (acc._1 :+ join, acc._2)
              }

          }
        case _ =>
          (acc._1 :+ join, acc._2)
      }
    }
    (analysis.copy(joins = rwJoins), rus)
  }

  /**
    * The tree q is successfully rewritten and returned in the first tuple element if
    * the second tuple element is not empty which can either be an exact tree match or a prefix tree match.
    * Otherwise, the original q is returned.
    */
  def possibleRewrites(q: AnalysisTree, rollups: Map[RollupName, AnalysisTree], requireCompoundRollupHint: Boolean): (AnalysisTree, Seq[String]) = {
    if (requireCompoundRollupHint && !compoundRollup(q.outputSchema.leaf)) {
      return (q, Seq.empty)
    }

    log.info("try rewrite compound query")
    // lazy view because only the first one rewritten one is taken
    // Might consider all and pick the one with the best score like simple rollup
    val rewritten = rollups.view.map {
      case (ruName, ruAnalyses) =>
        val rewritten = rewriteIfPrefix(q, ruAnalyses)
        (ruName, rewritten)
    }.filter(_._2.isDefined)

    rewritten.headOption match {
      case Some((ruName, Some(ruAnalyses))) =>
        (ruAnalyses, Seq(ruName))
      case _ =>
        (q, Seq.empty)
    }
  }

  private def rewriteExact(q: Analysis, qRightMost: Analysis): Analysis = {
    val columnMap = q.selection.values.zipWithIndex.map {
      case(expr, idx) =>
        (expr, ColumnRef(qualifier = None, column = rollupColumnId(idx), expr.typ.t)(expr.position))
    }.toMap
    val mappedSelection = q.selection.mapValues { expr => columnMap(expr)}
    q.copy(
      isGrouped = false ,
      distinct = Indistinct[ColumnId, SoQLType],
      selection = mappedSelection,
      joins = Nil,
      where = None,
      groupBys = Nil,
      having = None,
      orderBys = Nil,
      limit = qRightMost.limit,
      offset = qRightMost.offset,
      search = None,
      hints = Nil)
  }

  private def rewriteIfPrefix(q: AnalysisTree, r: AnalysisTree): Option[AnalysisTree] = {
    rewriteIfPrefix(q, r, rightMost(q))
  }

  private def rewriteIfPrefix(q: AnalysisTree, r: AnalysisTree, qRightMost: Leaf[Analysis]): Option[AnalysisTree] = {
    q match {
      case c@Compound(_, ql, qr) =>
        if (isEqual(q, r, qRightMost)) {
          val rewritten = rewriteExact(q.outputSchema.leaf, qRightMost.leaf)
          Some(Leaf(rewritten))
        } else if (isEqual(ql, r, qRightMost)) {
          val rewritten = rewriteExact(ql.outputSchema.leaf, qRightMost.leaf)
          Some(PipeQuery(Leaf(rewritten), qr))
        } else {
          rewriteIfPrefix(ql, r, qRightMost) match {
            case Some(result@Compound(_, _, _)) =>
              Some(Compound(c.op, result, qr))
            case result =>
              result
          }
        }
      case Leaf(_) if isEqual(q, r, qRightMost)=>
        val rewritten = rewriteExact(q.outputSchema.leaf, qRightMost.leaf)
        Some(Leaf(rewritten))
      case _ =>
        None
    }
  }

  /**
    * q is equal r ignoring limit and offset of the right most of q
    */
  private def isEqual(q: AnalysisTree, r: AnalysisTree, qRightMost: Leaf[Analysis]): Boolean = {
    (q, r) match {
      case (Compound(qo, ql, qr), Compound(ro, rl, rr)) if qo == ro =>
        isEqual(ql, rl, qRightMost) && isEqual(qr, rr, qRightMost)
      case (Leaf(qf), Leaf(rf)) =>
        if (q.eq(qRightMost)) qf.copy(limit = None, offset = None) == rf
        else qf == rf
      case _ =>
        false
    }
  }

  private def rightMost[T](bt: BinaryTree[T]): Leaf[T] = {
    bt match {
      case Compound(_, _, r) => rightMost(r)
      case l@Leaf(_) => l
    }
  }

  private def compoundRollup(q: Analysis): Boolean = {
    q.hints.exists {
      case CompoundRollup(_) => true
      case _ => false
    }
  }

}
