package com.socrata.querycoordinator.util

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
}
