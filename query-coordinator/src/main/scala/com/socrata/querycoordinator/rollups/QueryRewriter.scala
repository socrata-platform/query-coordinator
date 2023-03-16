package com.socrata.querycoordinator.rollups

import com.socrata.querycoordinator.rollups.QueryRewriter.{Analysis, AnalysisTree, Expr, RollupName}
import com.socrata.querycoordinator.util.BinaryTreeHelper
import com.socrata.querycoordinator.{Schema, SchemaWithFieldName}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.{BinaryTree, Leaf, PipeQuery, SoQLAnalysis, typed}
import com.socrata.soql.types.SoQLType

/**
  *
  * This is the interface of the QueryRewriter
  *
  * Any class wishing to use the QueryRewriter must use the interface, not a concrete implementation.
  */
abstract class QueryRewriter {

  /**
    * *Map rollup definitions into query analysis in schema context
    *
    * ToDo: Should this return merged analysis?
    *
    */
  def analyzeRollups(schema: Schema, rollups: Seq[RollupInfo],
                     getSchemaByTableName: TableName => SchemaWithFieldName): Map[RollupName, AnalysisTree]

  /**
    * Creates possible rewrites for the query given the rollups
    *
    */
  def possibleRewrites(q: Analysis, rollups: Map[RollupName, Analysis]): Map[RollupName, Analysis]
  def possibleRewrites(q: AnalysisTree, rollups: Map[RollupName, AnalysisTree], requireCompoundRollupHint: Boolean): (AnalysisTree, Seq[String])
  def possibleRewrites(q: Analysis, rollups: Map[RollupName, Analysis], debug: Boolean): Map[RollupName, Analysis]

  /**
    * Returns best scored rollup
    *
    */
  def bestRollup(rollups: Seq[(RollupName, Analysis)]): Option[(RollupName, Analysis)]

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
    * Merge rollups analysis
    */
  def mergeRollupsAnalysis(rus: Map[RollupName, AnalysisTree]): Map[RollupName, Analysis] = {
    rus.mapValues { bt =>
      val merged = doMerge(
        bt.map(a => a.mapColumnIds((columnId, _) => ColumnName(columnId)))
      )

      val mergedAgain = SoQLAnalysis.merge(
        SoQLFunctions.And.monomorphic.get,
        merged
      )
//
//      val mergedMap = merged match {
//        case PipeQuery(l, r) =>
//          l.outputSchema.leaf.selection.foreach { case (b,c) =>
//            BinaryTreeHelper.replace(r, r.outputSchema.leaf, r.outputSchema.leaf.copy(selection = {
//              r.outputSchema.leaf.selection.map(doRemap(l.outputSchema.leaf.selection))
//            }))
//          }
//        case x => x
//      }
//            val mergedMap = merged match {
//              case PipeQuery(l, r) => BinaryTreeHelper.replace(r, r.outputSchema.leaf, r.outputSchema.leaf.copy(selection = {
//                r.outputSchema.leaf.selection.map(doRemap(l.outputSchema.leaf.selection))
//              }))
//              case x => x
//            }

      merged.outputSchema.leaf.mapColumnIds((columnName, _) => columnName.name)
    }
  }

  def doRemap(schema:OrderedMap[ColumnName, typed.CoreExpr[ColumnName, SoQLType]])(selection:(ColumnName, typed.CoreExpr[ColumnName, SoQLType])):(ColumnName, typed.CoreExpr[ColumnName, SoQLType])={
    val (columnName,expression) = selection
    ???
  }

  def doMerge(analysisTree: BinaryTree[SoQLAnalysis[ColumnName, SoQLType]]): BinaryTree[SoQLAnalysis[ColumnName, SoQLType]] = {
    SoQLAnalysis.merge(
      SoQLFunctions.And.monomorphic.get,
      analysisTree
    ) match {
      case PipeQuery(l @ PipeQuery(_, _), r) =>
        doMerge(PipeQuery(doMerge(l), r))
      case other => other
    }
  }


  def primaryRollup(names: Seq[String]): Option[String] = {
    names.filterNot(_.startsWith(TableName.SoqlPrefix)).headOption
  }
}
