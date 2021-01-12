package com.socrata.querycoordinator.fusion

import com.socrata.soql.{BinaryTree, SoQLAnalysis}
import com.socrata.soql.ast.Select
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType

object NoopFuser extends SoQLRewrite {

  def rewrite(parsedStmts: BinaryTree[Select],
              columnIdMapping: Map[ColumnName, String],
              schema: Map[String, SoQLType]): BinaryTree[Select] = {
    parsedStmts
  }

  protected def rewrite(select: Select): Select = select

  def postAnalyze(analyses: BinaryTree[SoQLAnalysis[ColumnName, SoQLType]]):
    BinaryTree[SoQLAnalysis[ColumnName, SoQLType]] = analyses
}
