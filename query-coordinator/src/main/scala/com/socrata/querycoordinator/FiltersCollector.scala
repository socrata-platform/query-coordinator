package com.socrata.querycoordinator

import com.socrata.NonEmptySeq
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.ast.ColumnOrAliasRef
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.typed.{ColumnRef, CoreExpr, FunctionCall}
import com.socrata.soql.types.SoQLType

trait FiltersCollector {

  def collectFilterColumns(analyses: NonEmptySeq[SoQLAnalysis[_, _]]): Set[ColumnRef[_, _]] = {
    analyses.seq.foldLeft(Set.empty[ColumnRef[_, _]]) { (acc, x) =>
      acc ++ collectFilterColumns(x)
    }
  }

  def collectFilterColumns(analysis: SoQLAnalysis[_, _]): Set[ColumnRef[_, _]] = {
    analysis.where.toSet.flatMap( (x: CoreExpr[_, _]) => collectFilterColumns(x))
  }

  def collectFilterColumns(expr: CoreExpr[_, _]): Set[ColumnRef[_, _]] = {
    expr match {
      case c: ColumnRef[_, _] =>
        val set: Set[ColumnRef[_, _]] = Set(c)
        set
      case fc: FunctionCall[_, _] =>
        fc.parameters.foldLeft(Set.empty[ColumnRef[_, _]]) { (acc, arg) =>
          acc ++ collectFilterColumns(arg)
        }
      case _ =>
        Set.empty[ColumnRef[_, _]]
    }
  }
}
