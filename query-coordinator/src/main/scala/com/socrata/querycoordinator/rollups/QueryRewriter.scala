package com.socrata.querycoordinator.rollups

import com.socrata.querycoordinator.rollups.QueryRewriter.{Analysis, AnalysisTree, Expr, RollupName}
import com.socrata.querycoordinator.{Schema, SchemaWithFieldName}
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.{BinaryTree, SoQLAnalysis, Leaf, Compound, UnionQuery}
import com.socrata.soql.types.SoQLType

/**
  *
  * This is the interface of the QueryRewriter
  *
  * Any class wishing to use the QueryRewriter must use the interface, not a concrete implementation.
  */
trait QueryRewriter {

  def possiblyRewriteOneAnalysisInQuery(dataset: String,
                                        schema: Schema,
                                        analyzedQuery: BinaryTree[SoQLAnalysis[String, SoQLType]],
                                        ruMapOpt: Option[Map[RollupName, AnalysisTree]],
                                        ruFetcher: () => Seq[RollupInfo],
                                        schemaFetcher: TableName => SchemaWithFieldName,
                                        debug: Boolean):
  (BinaryTree[SoQLAnalysis[String, SoQLType]], Seq[String])

}

object QueryRewriter {
  import com.socrata.soql.typed._ // scalastyle:ignore import.grouping

  val log = org.slf4j.LoggerFactory.getLogger(classOf[QueryRewriter])

  type Analysis = SoQLAnalysis[ColumnId, SoQLType]
  type AnalysisTree = BinaryTree[Analysis]
  type ColumnId = String
  type RollupName = String
  type Expr = CoreExpr[ColumnId, SoQLType]

  def rollupAtJoin(q: Analysis): Boolean = {
    q.hints.exists {
      case RollupAtJoin(_) => true
      case _ => false
    }
  }

  /**
    * Merge rollups analyses
    */
  def mergeRollupsAnalysis(rus: Map[RollupName, AnalysisTree]): Map[RollupName, Analysis] =
    rus.mapValues(mergeAnalysis).flatMap {
      case (rollupName, Leaf(analysis)) => Some(rollupName -> analysis)
      case (rollupName, Compound(_, _, _)) =>
        log.error(s"Found rollup that was not fully collapsible ${rollupName}")
        None
    }


  def mergeAnalysis(analysis: AnalysisTree): AnalysisTree = {
    val mergedTree = SoQLAnalysis.merge(
      SoQLFunctions.And.monomorphic.get,
      analysis.map(_.mapColumnIds((columnId, _) => ColumnName(columnId))
      )
    )
    mergedTree.map(_.mapColumnIds((columnName, _) => columnName.name))
  }

  def primaryRollup(names: Seq[String]): Option[String] = {
    names.filterNot(_.startsWith(TableName.SoqlPrefix)).headOption
  }
}
