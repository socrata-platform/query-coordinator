package com.socrata.querycoordinator.fusion

import com.socrata.NonEmptySeq
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.ast.Select
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType

object NoopFuser extends SoQLRewrite {

  def rewrite(parsedStmts: NonEmptySeq[Select],
              columnIdMapping: Map[ColumnName, String],
              schema: Map[String, SoQLType]): NonEmptySeq[Select] = {
    parsedStmts
  }

  protected def rewrite(select: Select): Select = select

  def postAnalyze(analyses: NonEmptySeq[SoQLAnalysis[ColumnName, SoQLType]]):
    NonEmptySeq[SoQLAnalysis[ColumnName, SoQLType]] = analyses
}
