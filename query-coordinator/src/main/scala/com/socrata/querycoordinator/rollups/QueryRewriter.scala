package com.socrata.querycoordinator.rollups

import com.socrata.querycoordinator.rollups.QueryRewriter.{Analysis, AnalysisTree, Expr, RollupName}
import com.socrata.querycoordinator.{Schema, SchemaWithFieldName}
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.{BinaryTree, SoQLAnalysis, Leaf, UnionQuery}
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
  def mergeRollupsAnalysis(rus: Map[RollupName, AnalysisTree]): Map[RollupName, Analysis] = {
    rus.mapValues(mergeAnalysis).collect({
      case (k, Some(v)) => k -> v
    })
  }

  private[querycoordinator] def mergeAnalysis(analysis: AnalysisTree): Option[Analysis] = {
    val m = SoQLAnalysis.merge(
      SoQLFunctions.And.monomorphic.get,
      analysis.map(a => a.mapColumnIds((columnId, _) => ColumnName(columnId))
      )
    )

    m match {
      case Leaf(_) => Some(m.outputSchema.leaf.mapColumnIds((columnName, _) => columnName.name))
      case _ => None
    }
  }

  def primaryRollup(names: Seq[String]): Option[String] = {
    names.filterNot(_.startsWith(TableName.SoqlPrefix)).headOption
  }
}
