package com.socrata.querycoordinator

import com.socrata.http.client.RequestBuilder
import com.socrata.querycoordinator.SchemaFetcher.{NoSuchDatasetInSecondary, Successful}
import com.socrata.querycoordinator.exceptions.JoinedDatasetNotColocatedException
import com.socrata.querycoordinator.fusion.{CompoundTypeFuser, SoQLRewrite}
import com.socrata.querycoordinator.util.BinaryTreeHelper
import com.socrata.soql.ast.{Select, SubSelect}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment._
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.parsing.{AbstractParser, Parser}
import com.socrata.soql.typed._
import com.socrata.soql.types.SoQLType
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery, SoQLAnalysis, SoQLAnalyzer, ast}
import com.typesafe.scalalogging.Logger
import org.joda.time.DateTime


class QueryParser(analyzer: SoQLAnalyzer[SoQLType], schemaFetcher: SchemaFetcher, maxRows: Option[Int], defaultRowsLimit: Int) {
  import QueryParser._ // scalastyle:ignore import.grouping
  import com.socrata.querycoordinator.util.Join._

  type AnalysisContext = Map[String, DatasetContext[SoQLType]]

  private def go(columnIdMapping: Map[ColumnName, String], schema: Map[String, SoQLType])
                (f: AnalysisContext => (BinaryTree[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String], DateTime)): Result = {

    val ds = toAnalysisContext(dsContext(columnIdMapping, schema))
    try {
      val (analyses, joinColumnIdMapping, dateTime) = f(ds)
      limitRows(analyses) match {
        case Right(analyses) =>
          val analysesInColumnIds = remapAnalyses(joinColumnIdMapping, analyses)
          SuccessfulParse(analysesInColumnIds, dateTime)
        case Left(result) => result
      }
    } catch {
      case e: SoQLException =>
        AnalysisError(e)
      case e: JoinedDatasetNotColocatedException =>
        QueryParser.JoinedTableNotFound(e.dataset, e.secondaryHost)
    }
  }

  /**
   * This function shares the same logic with soql-postgres-adapter::SoQLAnalyzerHelper.remapAnalyses
   * Convert analyses from column names to column ids.
   * Add new columns and remap "column ids" as it walks the soql chain.
   */
  private def remapAnalyses(columnIdMapping: Map[QualifiedColumnName, String],
                            analyses: BinaryTree[SoQLAnalysis[ColumnName, SoQLType]])
    : BinaryTree[SoQLAnalysis[String, SoQLType]] = {

    val newMapping: Map[(ColumnName, Qualifier), String] = columnIdMapping map {
      case (QualifiedColumnName(qualifier, columnName), userColumnId) =>
        ((columnName, qualifier), userColumnId)
    }

    def toColumnNameJoinAlias(joinAlias: Option[String], columnName: ColumnName) = (columnName, joinAlias)
    def toUserColumnId(columnName: ColumnName) = columnName.name

    analyses match {
      case PipeQuery(l, r) =>
        val nl = remapAnalyses(columnIdMapping, l)
        val prev = nl.outputSchema.leaf
        val ra = r.asLeaf.get
        val prevQueryAlias = ra.from match {
          case Some(TableName(TableName.This, alias@Some(_))) =>
            alias
          case _ =>
            None
        }
        val prevQColumnIdToQColumnIdMap = prev.selection.foldLeft(newMapping) { (acc, selCol) =>
          val (colName, _expr) = selCol
          acc + ((colName, prevQueryAlias) -> toUserColumnId(colName))
        }
        val nr = r.asLeaf.get.mapColumnIds(prevQColumnIdToQColumnIdMap, toColumnNameJoinAlias, toUserColumnId, toUserColumnId)
        PipeQuery(nl, Leaf(nr))
      case Compound(op, l, r) =>
        val la = remapAnalyses(columnIdMapping, l)
        val ra = remapAnalyses(columnIdMapping, r)
        Compound(op, la, ra)
      case Leaf(analysis) =>
        val newMappingThisAlias = analysis.from match {
          case Some(tn@TableName(TableName.This, Some(_))) =>
            newMapping.foldLeft(newMapping) { (acc, mapEntry) =>
              mapEntry match {
                case ((columnName, None), userColumnId) =>
                  acc ++ Map((columnName, Some(tn.qualifier)) -> userColumnId,
                    (columnName, Some(TableName.This)) -> userColumnId)
                case _ =>
                  acc
              }
            }
          case _ =>
            newMapping
        }

        val remapped: SoQLAnalysis[String, SoQLType] = analysis.mapColumnIds(newMappingThisAlias, toColumnNameJoinAlias, toUserColumnId, toUserColumnId)
        Leaf(remapped)
    }
  }

  private def limitRows(analyses: BinaryTree[SoQLAnalysis[ColumnName, SoQLType]])
    : Either[Result, BinaryTree[SoQLAnalysis[ColumnName, SoQLType]]] = {
    val last = analyses.outputSchemaLeaf
    last.limit match {
      case Some(lim) =>
        val actualMax = BigInt(maxRows.map(_.toLong).getOrElse(Long.MaxValue))
        if (lim <= actualMax) { Right(analyses) }
        else { Left(RowLimitExceeded(actualMax)) }
      case None =>
        val outermostAnalyses: Seq[SoQLAnalysis[ColumnName, SoQLType]] = BinaryTreeHelper.outerMostAnalyses(analyses)
        val last: SoQLAnalysis[ColumnName, SoQLType] = outermostAnalyses match {
          case Seq(one) =>
            one
          case moreThanOne: Seq[_] =>
            moreThanOne.last
        }
        val lastWithLimit = last.copy(limit = Some(defaultRowsLimit))
        Right(BinaryTreeHelper.replace(analyses, last, lastWithLimit))
    }
  }

  private def analyzeQuery(query: String, columnIdMap: Map[ColumnName, String],
                           selectedSecondaryInstanceBase: RequestBuilder,
                           fuser: SoQLRewrite,
                           schema: Map[String, SoQLType])
    : AnalysisContext => (BinaryTree[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String], DateTime) = {
    analyzeFullQuery(query, columnIdMap, selectedSecondaryInstanceBase, fuser, schema)
  }

  private def analyzeFullQuery(query: String,
                               columnIdMap: Map[ColumnName, String],
                               selectedSecondaryInstanceBase: RequestBuilder,
                               fuser: SoQLRewrite,
                               schema: Map[String, SoQLType])(ctx: AnalysisContext)
    : (BinaryTree[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String], DateTime) = {
    val parserParams =
      new AbstractParser.Parameters(
        allowJoins = true,
        systemColumnAliasesAllowed = systemColumns ++ columnIdMap.keySet.filter(_.caseFolded.startsWith(":@")))
    val parsed = new Parser(parserParams).binaryTreeSelect(query)

    val (primaryAlias, ctxWithAlias) = parsed.leftMost.leaf.from match {
      case Some(tn@TableName(name, a@Some(alias))) if name == TableName.This =>
        val primarySchema = ctx(TableName.PrimaryTable.name)
        (a, (ctx + (alias -> primarySchema)) + (name -> primarySchema))
      case _ =>
        (None, ctx)
    }

    val primaryColumnIdMap = columnIdMap.map { case (k, v) => QualifiedColumnName(primaryAlias, k) -> v } ++
      (if (primaryAlias.isDefined) columnIdMap.map { case (k, v) => QualifiedColumnName(Some(TableName.This), k) -> v }
      else Map.empty)

    val tableNames = collectTableNames(parsed)

    val (allColumnIdMap, allCtx, largestLastModified) =
      fetchRelatedTables(tableNames, selectedSecondaryInstanceBase, primaryColumnIdMap, ctxWithAlias)

    val rewritten = fuser.rewrite(parsed, columnIdMap, schema)
    val result = analyzer.analyzeBinary(rewritten)(allCtx)
    val fusedResult = fuser.postAnalyze(result)
    (fusedResult, allColumnIdMap, largestLastModified)
  }

  private def collectTableNames(selects: BinaryTree[Select]): Set[String] = {
    selects match {
      case Compound(_, l, r) =>
        collectTableNames(l) ++ collectTableNames(r)
      case Leaf(select) =>
        select.joins.foldLeft(select.from.map(_.name).filter(_ != TableName.This).toSet) { (acc, join) =>
          join.from.subSelect match {
            case Left(TableName(name, _)) =>
              acc + name
            case Right(SubSelect(selects, _)) =>
              acc ++ collectTableNames(selects)
          }
        }
    }
  }

  private def fetchRelatedTables(tableNames: Set[String],
                                 selectedSecondaryInstanceBase: RequestBuilder,
                                 primaryColumnIdMap: Map[QualifiedColumnName, String],
                                 ctx: AnalysisContext)
    : (Map[QualifiedColumnName, String], AnalysisContext, DateTime) = {
    tableNames.foldLeft((primaryColumnIdMap, ctx, new DateTime(0))) { (acc, tableName) =>
      val (accColumnIdMap, accCtx, accLastModified) = acc
      val schemaResult = schemaFetcher(selectedSecondaryInstanceBase, tableName, None, useResourceName = true)
      schemaResult match {
        case Successful(schema, _, _, lastModified) =>
          val combinedCtx = accCtx + (tableName -> schemaToDatasetContext(schema))
          val combinedIdMap = accColumnIdMap ++ schema.schema.map {
            case (columnId, (_, fieldName)) =>
              QualifiedColumnName(Some(tableName), new ColumnName(fieldName)) -> columnId
          }
          val largestLastModified = if (lastModified.isAfter(accLastModified)) lastModified else accLastModified
          (combinedIdMap, combinedCtx, largestLastModified)
        case NoSuchDatasetInSecondary =>
          throw new JoinedDatasetNotColocatedException(tableName, selectedSecondaryInstanceBase.host)
        case other =>
          // TODO: Keeping original semantic.  Improve error handling later.
          logger.error(s"failed to fetch schema ${other}: ${tableName} ${selectedSecondaryInstanceBase.host}")
          acc
      }
    }
  }

  private def schemaToDatasetContext(schema: SchemaWithFieldName): DatasetContext[SoQLType] = {
    val columnNameTypes = schema.schema.values.foldLeft(Map.empty[ColumnName, SoQLType]) { (acc, v) =>
      v match {
        case (soqlType, fieldName) =>
          acc + (new ColumnName(fieldName) -> soqlType)
      }
    }
    val schemaForOrderedMap = columnNameTypes.mapValues( x => (x.hashCode, x ))

    new DatasetContext[SoQLType] {
      val schema: OrderedMap[ColumnName, SoQLType] = new OrderedMap(schemaForOrderedMap, schemaForOrderedMap.keys.toVector)
    }
  }

  def apply(query: String,
            columnIdMapping: Map[ColumnName, String],
            schema: Map[String, SoQLType],
            selectedSecondaryInstanceBase: RequestBuilder,
            fuseMap: Map[String, String] = Map.empty,
            merged: Boolean = true): Result = {
    val compoundTypeFuser = CompoundTypeFuser(fuseMap)
    val postAnalyze = analyzeQuery(query, columnIdMapping, selectedSecondaryInstanceBase, compoundTypeFuser, schema)
    val analyzeMaybeMerge = if (false && merged) { postAnalyze andThen soqlMerge } else { postAnalyze }
    go(columnIdMapping, schema)(analyzeMaybeMerge)
  }

  private def soqlMerge(analysesAndColumnIdMap: (BinaryTree[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String], DateTime))
    : (BinaryTree[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String], DateTime) = {
    val (analyses, qualColumnIdMap, dateTime) = analysesAndColumnIdMap
    (SoQLAnalysis.merge(andFn, analyses), qualColumnIdMap, dateTime)
  }
}

object QueryParser {
  private val logger = Logger[QueryParser]

  sealed abstract class Result

  case class SuccessfulParse(analyses: BinaryTree[SoQLAnalysis[String, SoQLType]], largestLastModifiedOfJoins: DateTime) extends Result

  case class AnalysisError(problem: SoQLException) extends Result

  case class UnknownColumnIds(columnIds: Set[String]) extends Result

  case class RowLimitExceeded(max: BigInt) extends Result

  case class JoinedTableNotFound(joinedTable: String, secondaryHost: String) extends Result

  /**
   * Make schema which is a mapping of column name to datatype
   * by going through the raw schema of column id to datatype map.
   * Ignore columns that exist in truth but missing in secondary.
   * @param columnIdMapping column name to column id map (supposedly from soda fountain)
   * @param rawSchema column id to datatype map like ( xxxx-yyyy -> text, ... ) (supposedly from secondary)
   * @return column name to datatype map like ( field_name -> text, ... )
   */
  def dsContext(columnIdMapping: Map[ColumnName, String],
                rawSchema: Map[String, SoQLType]): DatasetContext[SoQLType] = {
    val knownColumnIdMapping = columnIdMapping.filter { case (k, v) => rawSchema.contains(v) }
    if (columnIdMapping.size != knownColumnIdMapping.size) {
      logger.warn(s"truth has columns unknown to secondary ${columnIdMapping.size} ${knownColumnIdMapping.size}")
    }
    new DatasetContext[SoQLType] {
      val schema: OrderedMap[ColumnName, SoQLType] =
        OrderedMap(knownColumnIdMapping.mapValues(rawSchema).toSeq.sortBy(_._1): _*)
    }
  }

  // And function is used for chain SoQL merge.
  private val andFn = SoQLFunctions.And.monomorphic.get

  private val systemColumns = Set(":id", ":created_at", ":updated_at").map(ColumnName(_))
}
