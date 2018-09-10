package com.socrata.querycoordinator.util

import com.socrata.soql.ast
import com.socrata.soql.ast.{BasedSelect, From, Select, SimpleSelect}
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.typed.Qualifier

object Join {

  val NoQualifier: Qualifier = None

  // TODO: Join - from one to multiple contexts
  def toAnalysisContext[ContextType](ctx: ContextType): Map[String, ContextType] = {
    Map(TableName.PrimaryTable.name -> ctx)
  }


  // TODO: Join - qualifier
  def mapIgnoringQualifier[NewColumnId](map: ColumnName => NewColumnId)(columnName: ColumnName, qualifier: Qualifier)
    : NewColumnId = {
    map(columnName)
  }

  def expandJoins(join: ast.Join): List[ast.Join] = {
    if (SimpleSelect.isSimple(join.from)) List(join)
    else expandJoins(join.from) :+ join
  }

  def sourceSource(ts: com.socrata.soql.environment.TableSource) = {
    ts match {
      case bs: BasedSelect => List(bs.decontextualized)
      case _ => Nil
    }
  }

  def expandJoins(selects: List[Select]): List[ast.Join] = {
    selects.flatMap(s => s.joins.flatMap(expandJoins))
  }

  def expandJoins(from: From): List[ast.Join] = {
    expandJoins(from.refinements ++ sourceSource(from.source))
  }
}
