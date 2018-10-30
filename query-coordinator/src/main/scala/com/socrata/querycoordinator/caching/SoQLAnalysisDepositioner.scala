package com.socrata.querycoordinator.caching

import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.typed._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, TableName}

import scala.util.parsing.input.NoPosition

object SoQLAnalysisDepositioner {
  def apply[ColumnId,Type](sa: SoQLAnalysis[ColumnId,Type]): SoQLAnalysis[ColumnId,Type] = {
    val SoQLAnalysis(isGrouped, distinct, selection, joins, where, groupBys, having, orderBys, limit, offset, search) = sa
    SoQLAnalysis(isGrouped = isGrouped,
                 distinct = distinct,
                 selection = depositionSelection(selection),
                 joins = depsoitionOptJoins(joins),
                 where = depositionOptExpr(where),
                 groupBys = depositionGroupBys(groupBys),
                 having = depositionOptExpr(having),
                 orderBys = depositionOrderBys(orderBys),
                 limit = limit,
                 offset = offset,
                 search = search)
  }

  private def depositionSelection[ColumnId,Type](selection: OrderedMap[ColumnName, CoreExpr[ColumnId, Type]]) = {
    selection.mapValues(depositionExpr)
  }

  private def depositionExpr[ColumnId,Type](expr: CoreExpr[ColumnId, Type]): CoreExpr[ColumnId, Type] = {
    expr match {
      case ColumnRef(qual, column, typ) => ColumnRef(qual, column, typ)(NoPosition)
      case NumberLiteral(value, typ) => NumberLiteral(value, typ)(NoPosition)
      case StringLiteral(value, typ) => StringLiteral(value, typ)(NoPosition)
      case BooleanLiteral(value, typ) => BooleanLiteral(value, typ)(NoPosition)
      case NullLiteral(typ) => NullLiteral(typ)(NoPosition)
      case fc: FunctionCall[ColumnId, Type] => FunctionCall[ColumnId, Type](fc.function, fc.parameters.map(depositionExpr))(NoPosition, NoPosition)
    }
  }

  private def depositionOptExpr[ColumnId,Type](expr: Option[CoreExpr[ColumnId, Type]]) = expr.map(depositionExpr)

  private def depositionGroupBys[ColumnId,Type](expr: Seq[CoreExpr[ColumnId, Type]]) = expr.map(depositionExpr)

  private def depositionOrderBys[ColumnId,Type](expr: Seq[OrderBy[ColumnId, Type]]) = expr.map(depositionOrderBy)

  private def depositionOrderBy[ColumnId,Type](ob: OrderBy[ColumnId, Type]) = ob.copy(expression = depositionExpr(ob.expression))

  private def depsoitionOptJoins[ColumnId, Type](joins: Seq[Join[ColumnId, Type]]) = {
    joins.map { join =>
      val mappedSubAna = join.from.subAnalysis.map(sa => sa.copy(analyses = sa.analyses.map(SoQLAnalysisDepositioner.apply)))
      Join(join.typ, join.from.copy(subAnalysis = mappedSubAna), depositionExpr(join.on))
    }
  }
}
