package com.socrata.querycoordinator.fusion

import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.ast.Select
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType

object NoopFuser extends SoQLRewrite {

  def rewrite(parsedStmts: List[Select],
              columnIdMapping: Map[ColumnName, String],
              schema: Map[String, SoQLType]): List[Select] = {
    parsedStmts
  }

  protected def rewrite(select: Select): Select = select

  def postAnalyze(analyses: Seq[SoQLAnalysis[ColumnName, SoQLType]]):
    List[SoQLAnalysis[ColumnName, SoQLType]] = analyses.toList
}
