package com.socrata.querycoordinator

import com.socrata.NonEmptySeq
import com.socrata.soql.ast._
import com.socrata.soql.environment.TableName

object UniqueTableAliases {

  /**
    * Make every table alias unqiue by appending index as suffix.
    * If query processing is improved to isolate table alias namespace between two selects in a chained SoQL,
    * renaming table aliases is not necessary.
    */
  def apply(selects: NonEmptySeq[Select]): NonEmptySeq[Select] = {

    if (selects.seq.exists(_.joins.nonEmpty)) {
      val (_, newSelects, _) = selects.seq.foldLeft((0, Seq.empty[Select], Map.empty[String, String])) { (acc, select) =>
        val (index, sels, map) = acc
        val (newSelect, newMap) = if (select.joins.nonEmpty) replaceAlias(select, index, map) else (select, map)
        (index + 1, sels :+ newSelect, newMap)
      }

      NonEmptySeq(newSelects.head, newSelects.tail)
    } else {
      selects
    }
  }

  private def replaceAlias(select: Select, index: Int, tableAliasMap: Map[String, String]): (Select, Map[String, String]) = {
    val (newJoins, aliasMap) = select.joins.foldLeft((Seq.empty[Join], tableAliasMap)) { (acc, join) =>
      val (joins, map) = acc
      join.from.alias.map { fromAlias =>
        val toAlias = if (map.contains(fromAlias)) fromAlias + "__" + index else fromAlias
        val newMap = map + (fromAlias -> toAlias)
        (joins :+ replaceAlias(join, newMap), newMap)
      }.getOrElse((joins :+ join, map))
    }

    val reAliasedSelection = select.selection.copy(expressions =
      select.selection.expressions.map(se => se.copy(expression = replaceAlias(se.expression, aliasMap))))

    val newSelect = select.copy(
      selection = reAliasedSelection,
      joins = newJoins,
      where = select.where.map(w => replaceAlias(w, aliasMap)),
      groupBys = select.groupBys.map(e => replaceAlias(e, aliasMap)),
      having = select.having.map(h => replaceAlias(h, aliasMap)),
      orderBys = select.orderBys.map(o => o.copy(expression =  replaceAlias(o.expression, aliasMap)))
    )

    (newSelect, aliasMap)
  }

  private def replaceAlias(join: Join, map: Map[String, String]): Join = {

    val newFromAlias = join.from.alias.map(x => map.getOrElse(x, x))
    val newSubSelect = join.from.subSelect.map(x => x.copy(alias = map.getOrElse(x.alias, x.alias)))
    val newTableName: TableName = join.from.fromTable.copy(alias = newFromAlias)
    val newJoinSelect = JoinSelect(newTableName, newSubSelect)

    join match {
      case j: InnerJoin =>
        j.copy(newJoinSelect, replaceAlias(join.on, map))
      case j: LeftOuterJoin =>
        j.copy(newJoinSelect, replaceAlias(join.on, map))
      case j: RightOuterJoin =>
        j.copy(newJoinSelect, replaceAlias(join.on, map))
      case j: FullOuterJoin =>
        j.copy(newJoinSelect, replaceAlias(join.on, map))
    }
  }

  private def replaceAlias(expr: Expression, map: Map[String, String]): Expression = {
    expr match {
      case x: ColumnOrAliasRef =>
        val q = x.qualifier.map(x => map.getOrElse(x, x))
        x.copy(qualifier = q)(x.position)
      case x: FunctionCall =>
        val ps = x.parameters.map(p => replaceAlias(p, map))
        x.copy(parameters = ps)(x.position, x.functionNamePosition)
      case x =>
        x
    }
  }
}
