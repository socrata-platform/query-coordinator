package com.socrata.querycoordinator.rollups

import com.socrata.querycoordinator.rollups.QueryRewriter.{Anal, Expr, RollupName}
import com.socrata.querycoordinator.{Schema, SchemaWithFieldName}
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.functions.{SoQLFunctions}
import com.socrata.soql.{BinaryTree, SoQLAnalysis}
import com.socrata.soql.types.{SoQLType}

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
                     getSchemaByTableName: TableName => SchemaWithFieldName): Map[RollupName, BinaryTree[Anal]]

  /**
    * Creates possible rewrites for the query given the rollups
    *
    */
  def possibleRewrites(q: Anal, rollups: Map[RollupName, Anal]): Map[RollupName, Anal]
  def possibleRewrites(q: BinaryTree[Anal], rollups: Map[RollupName, BinaryTree[Anal]], requireCompoundRollupHint: Boolean): (BinaryTree[Anal], Seq[String])
  def possibleRewrites(q: Anal, rollups: Map[RollupName, Anal], debug: Boolean): Map[RollupName, Anal]

  /**
    * Returns best scored rollup
    *
    */
  def bestRollup(rollups: Seq[(RollupName, Anal)]): Option[(RollupName, Anal)]

}

object QueryRewriter {
  import com.socrata.soql.typed._ // scalastyle:ignore import.grouping

  type Anal = SoQLAnalysis[ColumnId, SoQLType]
  type ColumnId = String
  type RollupName = String
  type Expr = CoreExpr[ColumnId, SoQLType]

  def rollupAtJoin(q: Anal): Boolean = {
    q.hints.exists {
      case RollupAtJoin(_) => true
      case _ => false
    }
  }

  /**
    * Merge rollups analysis
    */
  def mergeRollupsAnalysis(rus: Map[RollupName, BinaryTree[Anal]]): Map[RollupName, Anal] = {
    rus.mapValues(bt =>
      SoQLAnalysis.merge(
        SoQLFunctions.And.monomorphic.get,
        bt.map(a => a.mapColumnIds((columnId, _) => ColumnName(columnId))
        )
      ).outputSchema.leaf.mapColumnIds((columnName, _) => columnName.name)
    )
  }

  def primaryRollup(names: Seq[String]): Option[String] = {
    names.filterNot(_.startsWith(TableName.SoqlPrefix)).headOption
  }
}

