package com.socrata.querycoordinator

import com.socrata.NonEmptySeq
import com.socrata.http.client.RequestBuilder
import com.socrata.querycoordinator.SchemaFetcher.{NoSuchDatasetInSecondary, Successful}
import com.socrata.querycoordinator.exceptions.JoinedDatasetNotColocatedException
import com.socrata.querycoordinator.fusion.{CompoundTypeFuser, SoQLRewrite}
import com.socrata.soql.ast.{Select, SubSelect}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment._
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.parsing.{AbstractParser, Parser}
import com.socrata.soql.typed._
import com.socrata.soql.types.SoQLType
import com.socrata.soql.{BinaryTree, Compound, PipeQuery, SoQLAnalysis, SoQLAnalyzer, ast}
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
   * Convert analyses from column names to column ids.
   * Add new columns and remap "column ids" as it walks the soql chain.
   */
  private def remapAnalyses(columnIdMapping: Map[QualifiedColumnName, String], //schema: Map[String, SoQLType],
                            analyses: BinaryTree[SoQLAnalysis[ColumnName, SoQLType]]): BinaryTree[SoQLAnalysis[String, SoQLType]] = {

    // TODO: WIP
    val initialAcc = (columnIdMapping, Option.empty[SoQLAnalysis[String, SoQLType]])

    def mapWithNewColumns(analysis: SoQLAnalysis[ColumnName, SoQLType],
                          mapping: Map[QualifiedColumnName, String]): Map[QualifiedColumnName, String] = {
      // Newly introduced columns will be used as column id as is.
      // There should be some sanitizer upstream that checks for field_name conformity.
      // TODO: Alternatively, we may need to use internal column name map for new and temporary columns
      val newlyIntroducedColumns = analysis.selection.keys.map(QualifiedColumnName(None, _)).filter {
        columnName => !mapping.contains(columnName)
      }

      newlyIntroducedColumns.foldLeft(mapping) { (acc, newColumn) =>
        acc + (newColumn -> newColumn.columnName.name)
      }
    }

    def mapAnalysis(analysis: SoQLAnalysis[ColumnName, SoQLType],
                    mapping: Map[QualifiedColumnName, String]): SoQLAnalysis[String, SoQLType] = {
      val newMappingTupled = mapping.map { case (k, v) => (k.columnName, k.qualifier) -> v }
      def toColumnNameJoinAlias(joinAlias: Option[String], columnName: ColumnName) = (columnName, joinAlias)

      analysis.mapColumnIds(newMappingTupled, toColumnNameJoinAlias, _.name, _.name)
    }


    def mapAnalyses(mapping: Map[QualifiedColumnName, String], prevAna: Option[SoQLAnalysis[String, SoQLType]],
                    analysis: SoQLAnalysis[ColumnName, SoQLType]): (Map[QualifiedColumnName, String], SoQLAnalysis[String, SoQLType]) = {
      val mappingWithNewColumns = mapWithNewColumns(analysis, mapping)
      // Re-map columns except for the innermost soql
      val newMapping = prevAna.map(_.selection.foldLeft(mapping) { case (acc, (colName, _)) =>
        acc + (QualifiedColumnName(None, colName) -> colName.name)
      }).getOrElse(mappingWithNewColumns)

      val mappedAnalysis = mapAnalysis(analysis, newMapping)

      (mappingWithNewColumns, mappedAnalysis)
    }

    analyses match {
      case x@PipeQuery(l, r) =>
        val la = remapAnalyses(columnIdMapping, l)
        val ra = r.asT
        val (_, nra) = mapAnalyses(columnIdMapping, Some(la.previous), ra)
        Compound(x.op, left = la, right = nra)
      case Compound(op, l, r) => // if op == "QUERYUNION" =>
        val la = remapAnalyses(columnIdMapping, l)
        val ra = remapAnalyses(columnIdMapping, r)
        Compound(op, left = la, right = ra)
      case analysis: SoQLAnalysis[ColumnName, SoQLType] =>
        val (_, a) = mapAnalyses(columnIdMapping, None, analysis)
        a
    }
//
//    analyses.scanLeft1(mapAnalyses(columnIdMapping, None, _)) {
//      case ((map, prevAna), ana) => mapAnalyses(map, Some(prevAna), ana)
//    }.map(_._2)

   // null
  }

  private def qualifiedColumnNameToColumnId(qualifiedColumnNameMap: Map[QualifiedColumnName, String])
                                           (columnName: ColumnName, qual: Option[String]): String = {
    val columnId = qualifiedColumnNameMap(QualifiedColumnName(qual, columnName))
    columnId
  }

  private def limitRows(analyses: BinaryTree[SoQLAnalysis[ColumnName, SoQLType]])
    : Either[Result, BinaryTree[SoQLAnalysis[ColumnName, SoQLType]]] = {
    //TODO: WIP
    Right(analyses)
//    val lastAnalysis = analyses.last
//    lastAnalysis.limit match {
//      case Some(lim) =>
//        val actualMax = BigInt(maxRows.map(_.toLong).getOrElse(Long.MaxValue))
//        if (lim <= actualMax) { Right(analyses) }
//        else { Left(RowLimitExceeded(actualMax)) }
//      case None =>
//        Right(analyses.replaceLast(lastAnalysis.copy(limit = Some(defaultRowsLimit))))
//    }
  }

  private def analyzeQuery(query: String, columnIdMap: Map[ColumnName, String],
                           selectedSecondaryInstanceBase: RequestBuilder,
                           fuser: SoQLRewrite,
                           schema: Map[String, SoQLType]):

    AnalysisContext => (BinaryTree[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String], DateTime) = {
    analyzeFullQuery(query, columnIdMap, selectedSecondaryInstanceBase, fuser, schema)
  }

  private def analyzeFullQuery(query: String,
                               columnIdMap: Map[ColumnName, String],
                               selectedSecondaryInstanceBase: RequestBuilder,
                               fuser: SoQLRewrite,
                               schema: Map[String, SoQLType])(ctx: AnalysisContext):

    (BinaryTree[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String], DateTime) = {


    val parserParams =
      new AbstractParser.Parameters(
        allowJoins = true,
        systemColumnAliasesAllowed = systemColumns ++ columnIdMap.keySet.filter(_.caseFolded.startsWith(":@")))
    val parsed0 = new Parser(parserParams).test(query)
    val parsed = UniqueTableAliases(parsed0)

    // TODO: Merge union & join
    val primaryColumnIdMap = columnIdMap.map { case (k, v) => QualifiedColumnName(None, k) -> v }

    val joins = ast.Join.expandJoins(parsed.seq)
    val tableNames = collectTableNames(parsed)

    val (unionColumnIdMap, unionCtx, largestLastModifiedOfUnions) = collectRelatedTable(parsed.seq, selectedSecondaryInstanceBase, primaryColumnIdMap, ctx)

    val (joinColumnIdMap, joinCtx, largestLastModifiedOfJoins) =
      tableNames.foldLeft((unionColumnIdMap, unionCtx, new DateTime(0))) { (acc, tableName) =>
        val TableName(name, alias) = tableName
        val schemaResult = schemaFetcher(selectedSecondaryInstanceBase, name, None, useResourceName = true)
        schemaResult match {
          case Successful(schema, copyNumber, dataVersion, lastModified) =>
            val joinTableRef = Some(name) // joinTableName.qualifier
            val combinedCtx = acc._2 + (name -> schemaToDatasetContext(schema))
            val combinedIdMap = acc._1 ++ schema.schema.map {
              case (columnId, (_, fieldName)) =>
                QualifiedColumnName(joinTableRef, new ColumnName(fieldName)) -> columnId
            }
            val largestLastModified = if (lastModified.isAfter(acc._3)) lastModified else acc._3
            (combinedIdMap, combinedCtx, largestLastModified)
          case NoSuchDatasetInSecondary =>
            throw new JoinedDatasetNotColocatedException(name, selectedSecondaryInstanceBase.host)
          case _ =>
            acc
        }
      }

    //TODO: Write unapply for NonEmptySeq

    parsed match {
//      case justOne if parsed.length == 1 =>
//        val rewrittenOne = fuser.rewrite(justOne, columnIdMap, schema).head
//        val result = analyzer.analyzeWithSelection(rewrittenOne)(joinCtx)
//        val fusedResult = fuser.postAnalyze(NonEmptySeq(result))
//        (fusedResult, joinColumnIdMap, largestLastModifiedOfJoins)
      case moreThanOne =>
        val moreThanOneRewritten = fuser.rewrite(moreThanOne, columnIdMap, schema)
        val result = analyzer.analyzeBinary(moreThanOneRewritten)(joinCtx)
        val fusedResult = fuser.postAnalyze(result)
        (fusedResult, joinColumnIdMap, largestLastModifiedOfJoins)
    }
  }

  private def collectTableNames(bt: BinaryTree[Select]): Set[TableName] = {
    bt match {
      case Compound(op, l, r) =>
        collectTableNames(l) ++ collectTableNames(r)
      case s: Select =>
        s.joins.foldLeft(s.from.toSet) { (acc, join) =>
          join.from.subSelect match {
            case Left(tableName) =>
              acc + tableName
            case Right(SubSelect(selects, alias)) =>
              acc ++ collectTableNames(selects)
          }
        }
    }
  }

  private def collectRelatedTable(selects: Seq[Select],
                                  selectedSecondaryInstanceBase: RequestBuilder,
                                  primaryColumnIdMap: Map[QualifiedColumnName, String],
                                  ctx: AnalysisContext,
                                  ): (Map[QualifiedColumnName, String], AnalysisContext, DateTime) = {
    selects.foldLeft((primaryColumnIdMap, ctx, new DateTime(0))) { (acc, join) =>
      join.from match {
        case None =>
          acc
        case Some(joinTableName) =>
          val schemaResult = schemaFetcher(selectedSecondaryInstanceBase, joinTableName.name, None, useResourceName = true)
          schemaResult match {
            case Successful(schema, copyNumber, dataVersion, lastModified) =>
              val joinTableRef = joinTableName.qualifier

              val joinAlias = joinTableName.alias.getOrElse(joinTableName.qualifier)
              val combinedCtx = acc._2 + (joinTableRef -> schemaToDatasetContext(schema)) +
                (joinAlias -> schemaToDatasetContext(schema))
              val combinedIdMap = acc._1 ++ schema.schema.map {
                case (columnId, (_, fieldName)) =>
                  QualifiedColumnName(Some(joinTableRef), new ColumnName(fieldName)) -> columnId
              } ++ schema.schema.map {
                case (columnId, (_, fieldName)) =>
                  QualifiedColumnName(Some(joinAlias), new ColumnName(fieldName)) -> columnId
              }
              val largestLastModified = if (lastModified.isAfter(acc._3)) lastModified else acc._3
              (combinedIdMap, combinedCtx, largestLastModified)
            case NoSuchDatasetInSecondary =>
              throw new JoinedDatasetNotColocatedException(joinTableName.name, selectedSecondaryInstanceBase.host)
            case _ =>
              acc
          }
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
