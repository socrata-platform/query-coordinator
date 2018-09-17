package com.socrata.querycoordinator.util

import com.socrata.soql.ast
import com.socrata.soql.ast.{Select, SimpleSelect}
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.typed.Qualifier

object Join {

  val NoQualifier: Qualifier = None

  // TODO: Join - from one to multiple contexts
  def toAnalysisContext[ContextType](ctx: ContextType): Map[String, ContextType] = {
    Map(TableName.PrimaryTable.qualifier -> ctx)
  }


  // TODO: Join - qualifier
  def mapIgnoringQualifier[NewColumnId](map: ColumnName => NewColumnId)(columnName: ColumnName, qualifier: Qualifier)
    : NewColumnId = {
    map(columnName)
  }

  def expandJoins(join: ast.Join): List[ast.Join] = {
    val subSelects = join.from.selects
    if (subSelects.isEmpty) List(join)
    else (join :: expandJoins(subSelects)).reverse // TODO: does it need to be reversed?
  }

  def expandJoins(selects: List[Select]): List[ast.Join] = {
    selects.flatMap(_.joins.flatMap(expandJoins))
  }
}
