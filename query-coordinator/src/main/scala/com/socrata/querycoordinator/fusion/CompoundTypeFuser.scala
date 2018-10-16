package com.socrata.querycoordinator.fusion

import com.ibm.icu.text.SimpleDateFormat.ContextType
import com.socrata.NonEmptySeq
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.aliases.AliasAnalysis
import com.socrata.soql.ast._
import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.environment.{ColumnName, UntypedDatasetContext}
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.parsing.standalone_exceptions.BadParse
import com.socrata.soql.types.SoQLType
import com.typesafe.scalalogging.slf4j.Logging

import scala.util.parsing.input.NoPosition

sealed trait FuseType

case object FTLocation extends FuseType
case object FTPhone extends FuseType
case object FTUrl extends FuseType
case object FTRemove extends FuseType

trait SoQLRewrite {
  def rewrite(parsedStmts: NonEmptySeq[Select],
              columnIdMapping: Map[ColumnName, String],
              schema: Map[String, SoQLType]): NonEmptySeq[Select]

  protected def rewrite(select: Select): Select

  def postAnalyze(analyses: NonEmptySeq[SoQLAnalysis[ColumnName, SoQLType]]): NonEmptySeq[SoQLAnalysis[ColumnName, SoQLType]]
}

object CompoundTypeFuser {
  def apply(fuseBase: Map[String, String]): SoQLRewrite = {
    if (fuseBase.nonEmpty) new CompoundTypeFuser(fuseBase)
    else NoopFuser
  }
}

/**
 * This class translates compound type expressions base on expanded columns.
 *
 * Example:
 *
 * SELECT location
 *  WHERE location.latitude = 1.1
 *
 *  is rewritten as
 *
 * SELECT location(location,location_address,location_city,location_state,location_zip) AS location
 *  WHERE point_latitude(location) = 1.1
 *
 * @param fuseBase contains the base name of expanded columns which need to be fused.
 */
class CompoundTypeFuser(fuseBase: Map[String, String]) extends SoQLRewrite with Logging {

  import com.socrata.querycoordinator.util.Join._

  private val ColumnPrefix = "#" // prefix should have no special meaning in regex.

  // Expand fuse map to include column sub-columns
  // for example, location will expand to include location_address, city, state, zip.
  private val fuse = fuseBase.foldLeft(Map.empty[String, FuseType]) { (acc, kv) =>
    kv match {
      case (name, "location") =>
        acc ++ Map(name -> FTLocation,
                   s"${name}_address" -> FTRemove,
                   s"${name}_city" -> FTRemove,
                   s"${name}_state" -> FTRemove,
                   s"${name}_zip" -> FTRemove)
      case (name, "phone") =>
        acc ++ Map(name -> FTPhone,
                   s"${name}_type" -> FTRemove)
      case (name, "url") =>
        acc ++ Map(name -> FTUrl,
                   s"${name}_description" -> FTRemove)
      case (name, typ) =>
        logger.warn(s"unrecognize fuse type {} for {}", typ, name)
        acc
    }
  }

  def rewrite(parsedStmts: NonEmptySeq[Select],
              columnIdMapping: Map[ColumnName, String],
              schema: Map[String, SoQLType]): NonEmptySeq[Select] = {

    val baseCtx = new UntypedDatasetContext {
      override val columns: OrderedSet[ColumnName] = {
        // exclude non-existing columns in the schema
        val existedColumns = columnIdMapping.filter { case (k, v) => schema.contains(v) }
        OrderedSet(existedColumns.keysIterator.toSeq: _*)
      }
    }

    def expand(select: Select, ctx: Map[String, UntypedDatasetContext]) = {
      val expandedSelection = AliasAnalysis.expandSelection(select.selection)(ctx)
      val expandedStmt = select.copy(selection = Selection(None, Seq.empty, expandedSelection))
      // Column names collected are for building the context for the following SoQL in chained SoQLs like
      // "SELECT phone as x,'Work' as x_type,name |> SELECT *"
      // It is currently done using expression.toString or explicit column aliases.
      // TODO: A more correct way to do this maybe Expression.toSyntheticIdentifierBase or AliasAnalysis.
      // TODO: Similar processes/issues may exist somewhere else.
      val columnNames = expandedStmt.selection.expressions.map { se =>
        se.name.map(_._1).getOrElse(ColumnName(se.expression.toString.replaceAllLiterally("`", "")))
      }
      val nextCtx = new UntypedDatasetContext {
        override val columns: OrderedSet[ColumnName] = OrderedSet(columnNames: _*)
      }
      (expandedStmt, toAnalysisContext(nextCtx))
    }

    val expandedStatements = parsedStmts.scanLeft1(h => expand(h, toAnalysisContext(baseCtx))){
      case ((_, ctx), s) => expand(s, ctx)
    }.map(_._1)

    // rewrite only the last statement.
    expandedStatements.replaceLast(rewrite(expandedStatements.last))
  }

  protected def rewrite(select: Select): Select = {
    val fusedSelectExprs = select.selection.expressions.flatMap {
      case SelectedExpression(expr: Expression, namePos) =>
        rewriteExpr(expr) match {
          case Some(rwExpr) =>
            val alias =
              if (expr.eq(rwExpr)) { // Do not change the original name if the expression is not rewritten.
                namePos
              } else {
                namePos.orElse(Some((ColumnName(ColumnPrefix + expr.toSyntheticIdentifierBase), NoPosition)))
              }
            Seq(SelectedExpression(rwExpr, alias))
          case None =>
            Seq.empty
        }
    }

    val fusedWhere = select.where.map(e => rewriteExpr(e).getOrElse(e))
    val fusedGroupBy = select.groupBys.map(rewriteExpr).flatten
    val fusedHaving = select.having.map(e => rewriteExpr(e).getOrElse(e))
    val fusedOrderBy = select.orderBys.map { ob =>
      rewriteExpr(ob.expression).map(e => ob.copy(expression = e))
    }.flatten

    select.copy(
      selection = select.selection.copy(expressions = fusedSelectExprs),
      where = fusedWhere,
      groupBys = fusedGroupBy,
      having = fusedHaving,
      orderBys = fusedOrderBy
    )
  }

  /**
   * Columns involved are prefixed during ast rewrite and removed after analysis to avoid column name conflicts.
   */
  def postAnalyze(analyses: NonEmptySeq[SoQLAnalysis[ColumnName, SoQLType]]): NonEmptySeq[SoQLAnalysis[ColumnName, SoQLType]] = {
    val last = analyses.last
    val newSelect = last.selection map {
      case (cn, expr) =>
        ColumnName(cn.name.replaceFirst(ColumnPrefix, "")) -> expr
    }

    analyses.updated(analyses.size - 1, last.copy(selection = newSelect))
  }

  private def rewriteExpr(expr: Expression): Option[Expression] = {
    expr match {
      case baseColumn@ColumnOrAliasRef(NoQualifier, name: ColumnName) =>
        fuse.get(name.name) match {
          case Some(FTLocation) =>
            val address = ColumnOrAliasRef(NoQualifier, ColumnName(s"${name.name}_address"))(NoPosition)
            val city = ColumnOrAliasRef(NoQualifier, ColumnName(s"${name.name}_city"))(NoPosition)
            val state = ColumnOrAliasRef(NoQualifier, ColumnName(s"${name.name}_state"))(NoPosition)
            val zip = ColumnOrAliasRef(NoQualifier, ColumnName(s"${name.name}_zip"))(NoPosition)
            val args = Seq(baseColumn, address, city, state, zip)
            val fc = FunctionCall(SoQLFunctions.Location.name, args)(NoPosition, NoPosition)
            Some(fc)
          case Some(FTPhone) =>
            val phoneType = ColumnOrAliasRef(NoQualifier, ColumnName(s"${name.name}_type"))(NoPosition)
            var args = Seq(baseColumn, phoneType)
            val fc = FunctionCall(SoQLFunctions.Phone.name, args)(NoPosition, NoPosition)
            Some(fc)
          case Some(FTUrl) =>
            val urlDescription = ColumnOrAliasRef(NoQualifier, ColumnName(s"${name.name}_description"))(NoPosition)
            var args = Seq(baseColumn, urlDescription)
            val fc = FunctionCall(SoQLFunctions.Url.name, args)(NoPosition, NoPosition)
            Some(fc)
          case Some(FTRemove) =>
            None
          case None =>
            Some(expr)
        }
      case fc@FunctionCall(fnName, Seq(ColumnOrAliasRef(NoQualifier, name: ColumnName)))
        if (Set(SoQLFunctions.PointToLatitude.name, SoQLFunctions.PointToLongitude.name).contains(fnName)) => Some(fc)
      case fc@FunctionCall(fnName, Seq(ColumnOrAliasRef(NoQualifier, name: ColumnName), StringLiteral(prop)))
        if fnName.name == SpecialFunctions.Subscript.name =>
        fuse.get(name.name) match {
          case Some(FTLocation) =>
            prop match {
              case "latitude" =>
                val args = Seq(ColumnOrAliasRef(NoQualifier, ColumnName(s"${name.name}"))(NoPosition))
                Some(FunctionCall(SoQLFunctions.PointToLatitude.name, args)(NoPosition, NoPosition))
              case "longitude" =>
                val args = Seq(ColumnOrAliasRef(NoQualifier, ColumnName(s"${name.name}"))(NoPosition))
                Some(FunctionCall(SoQLFunctions.PointToLatitude.name, args)(NoPosition, NoPosition))
              case "human_address" =>
                val args = Seq("address", "city", "state", "zip")
                  .map(subProp => ColumnOrAliasRef(NoQualifier, ColumnName(s"${name.name}_$subProp"))(NoPosition))
                Some(FunctionCall(SoQLFunctions.HumanAddress.name, args)(NoPosition, NoPosition))
              case _ =>
                throw BadParse("unknown location property", fc.position)
            }
          case Some(FTPhone) =>
            prop match {
              case "phone_number" =>
                Some(ColumnOrAliasRef(NoQualifier, ColumnName(name.name))(NoPosition))
              case "phone_type" =>
                Some(ColumnOrAliasRef(NoQualifier, ColumnName(s"${name.name}_type"))(NoPosition))
              case _ =>
                throw BadParse("unknown phone property", fc.position)
            }
          case Some(FTUrl) =>
            prop match {
              case "url" =>
                Some(ColumnOrAliasRef(NoQualifier, ColumnName(name.name))(NoPosition))
              case "description" =>
                Some(ColumnOrAliasRef(NoQualifier, ColumnName(s"${name.name}_description"))(NoPosition))
              case _ =>
                throw BadParse("unknown phone property", fc.position)
            }
          case Some(FTRemove) =>
            throw BadParse("subscript call on sub-column", fc.position)
          case _ =>
            Some(fc)
        }
      case fc@FunctionCall(fnName, params) =>
         val rwParams: Seq[Expression] = params.map(e => rewriteExpr(e).getOrElse(e))
         Some(fc.copy(parameters = rwParams)(fc.functionNamePosition, fc.position))
      case _ =>
        Some(expr)
    }
  }
}
