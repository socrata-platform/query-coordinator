package com.socrata.querycoordinator

import com.socrata.http.client.RequestBuilder
import com.socrata.querycoordinator.SchemaFetcher.{Successful, SuccessfulExtendedSchema}
import com.socrata.querycoordinator.fusion.{CompoundTypeFuser, NoopFuser, SoQLRewrite}
import com.socrata.soql.aliases.AliasAnalysis
import com.socrata.soql.ast.{Expression, Select, Selection}
import com.socrata.soql.collection.{OrderedMap, OrderedSet}
import com.socrata.soql.environment._
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.parsing.Parser
import com.socrata.soql.typed._
import com.socrata.soql.types.SoQLType
import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer}
import com.typesafe.scalalogging.slf4j.Logging


class QueryParser(analyzer: SoQLAnalyzer[SoQLType], schemaFetcher: SchemaFetcher, maxRows: Option[Int], defaultRowsLimit: Int) {
  import QueryParser._ // scalastyle:ignore import.grouping
  import com.socrata.querycoordinator.util.Join._

  type AnalysisContext = Map[String, DatasetContext[SoQLType]]

  private def go(columnIdMapping: Map[ColumnName, String], schema: Map[String, SoQLType])
                (f: AnalysisContext => (Seq[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String])): Result = {
    val ds = toAnalysisContext(dsContext(columnIdMapping, schema))
    try {
      val (analyses, joinColumnIdMapping) = f(ds)
      limitRows(analyses) match {
        case Right(analyses) =>
          val analysesInColumnIds = remapAnalyses(joinColumnIdMapping, analyses)
          SuccessfulParse(analysesInColumnIds)
        case Left(result) => result
      }
    } catch {
      case e: SoQLException =>
        AnalysisError(e)
    }
  }

  /**
   * Convert analyses from column names to column ids.
   * Add new columns and remap "column ids" as it walks the soql chain.
   */
  private def remapAnalyses(columnIdMapping: Map[QualifiedColumnName, String], //schema: Map[String, SoQLType],
                            analyses: Seq[SoQLAnalysis[ColumnName, SoQLType]])
    : Seq[SoQLAnalysis[String, SoQLType]] = {
    val initialAcc = (columnIdMapping, Seq.empty[SoQLAnalysis[String, SoQLType]])
    val (_, analysesInColIds) = analyses.foldLeft(initialAcc) { (acc, analysis) =>
      val (mapping, convertedAnalyses) = acc
      // Newly introduced columns will be used as column id as is.
      // There should be some sanitizer upstream that checks for field_name conformity.
      // TODO: Alternatively, we may need to use internal column name map for new and temporary columns
      val newlyIntroducedColumns = analysis.selection.keys.map(QualifiedColumnName(None, _)).filter { columnName => !mapping.contains(columnName) }
      val mappingWithNewColumns: Map[QualifiedColumnName, String] = newlyIntroducedColumns.foldLeft(mapping) { (acc, newColumn) =>
        acc + (newColumn -> newColumn.columnName.name)
      }
      // Re-map columns except for the innermost soql
      val newMapping: Map[QualifiedColumnName, String] =
        if (convertedAnalyses.nonEmpty) {
          val prevAnalysis = convertedAnalyses.last
          prevAnalysis.selection.foldLeft(mapping) { (acc, selCol) =>
            val (colName, expr) = selCol
            acc + (QualifiedColumnName(None, colName) -> colName.name)
          }
        } else {
          mappingWithNewColumns
        }

      val a: SoQLAnalysis[String, SoQLType] = analysis.mapColumnIds(qualifiedColumnNameToColumnId(newMapping))
      (mappingWithNewColumns, convertedAnalyses :+ a)
    }
    analysesInColIds
  }

  private def qualifiedColumnNameToColumnId(qualifiedColumnNameMap: Map[QualifiedColumnName, String])
                                           (columnName: ColumnName, qual: Option[String]): String = {
    val columnId = qualifiedColumnNameMap(QualifiedColumnName(qual, columnName))
    columnId
  }

  private def limitRows(analyses: Seq[SoQLAnalysis[ColumnName, SoQLType]])
    : Either[Result, Seq[SoQLAnalysis[ColumnName, SoQLType]]] = {
    val lastAnalysis = analyses.last
    lastAnalysis.limit match {
      case Some(lim) =>
        val actualMax = BigInt(maxRows.map(_.toLong).getOrElse(Long.MaxValue))
        if (lim <= actualMax) { Right(analyses) }
        else { Left(RowLimitExceeded(actualMax)) }
      case None =>
        Right(analyses.dropRight(1) :+ lastAnalysis.copy(limit = Some(defaultRowsLimit)))
    }
  }

  private def analyzeQuery(query: String, columnIdMap: Map[ColumnName, String], selectedSecondaryInstanceBase: RequestBuilder):
    AnalysisContext => (Seq[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String]) = {
    analyzeFullQuery(query, columnIdMap, selectedSecondaryInstanceBase)
  }

  private def analyzeFullQuery(query: String, columnIdMap: Map[ColumnName, String], selectedSecondaryInstanceBase: RequestBuilder)(ctx: AnalysisContext):
    (Seq[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String]) = {

    val parsed = new Parser().selectStatement(query)

    val joins = parsed.flatten { select =>
      select.join match {
        case None => Seq.empty
        case Some(joins) => joins
      }
    }

    val primaryColumnIdMap = columnIdMap.map { case (k, v) => QualifiedColumnName(None, k) -> v }

    val (joinColumnIdMap, joinCtx) =
      joins.foldLeft((primaryColumnIdMap, ctx)) { (acc, join) =>
        join match {
          case (joinResourceName, _) =>
            val schemaResult = schemaFetcher.schemaWithFieldName(selectedSecondaryInstanceBase, joinResourceName.name, None, useResourceName = true)
            schemaResult match {
              case SuccessfulExtendedSchema(schema, copyNumber, dataVersion, lastModified) =>
                val joinResourceAliasOrName = joinResourceName.alias.getOrElse(joinResourceName.name)
                val combinedCtx = acc._2 + (joinResourceAliasOrName ->  schemaToDatasetContext(schema))
                val combinedIdMap = acc._1 ++ schema.schema.map {
                  case (columnId, (_, fieldName)) =>
                    (QualifiedColumnName(Some(joinResourceAliasOrName), new ColumnName(fieldName)) -> columnId)
                }
                (combinedIdMap, combinedCtx)
              case _ =>
                acc
            }
        }
      }

    parsed match {
      case Seq(one) =>
        val result = analyzer.analyzeWithSelection(one)(joinCtx)
        (Seq(result), joinColumnIdMap)
      case moreThanOne =>
        val result = analyzer.analyze(moreThanOne)(joinCtx)
        (result, joinColumnIdMap)
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

  private def analyzeQueryWithCompoundTypeFusion(query: String,
                                                 fuser: SoQLRewrite,
                                                 columnIdMapping: Map[ColumnName, String],
                                                 schema: Map[String, SoQLType]):
    AnalysisContext => Seq[SoQLAnalysis[ColumnName, SoQLType]] = {
    val parsedStmts = new Parser().selectStatement(query)

    // expand "select *"
    val firstStmt = parsedStmts.head

    val baseCtx = new UntypedDatasetContext {
      override val columns: OrderedSet[ColumnName] = {
        // exclude non-existing columns in the schema
        val existedColumns = columnIdMapping.filter { case (k, v) => schema.contains(v) }
        OrderedSet(existedColumns.keysIterator.toSeq: _*)
      }
    }

    val expandedStmts = parsedStmts.foldLeft((Seq.empty[Select], toAnalysisContext(baseCtx))) { (acc, select) =>
      val (selects, ctx) = acc
      val expandedSelection = AliasAnalysis.expandSelection(select.selection)(ctx)
      val expandedStmt = select.copy(selection = Selection(None, None, expandedSelection))
      val columnNames = expandedStmt.selection.expressions.map { se =>
        se.name.map(_._1).getOrElse(ColumnName(se.expression.toString.replaceAllLiterally("`", "")))
      }
      val nextCtx = new UntypedDatasetContext {
        override val columns: OrderedSet[ColumnName] = OrderedSet(columnNames: _*)
      }
      (selects :+ expandedStmt, toAnalysisContext(nextCtx))
    }._1

    // rewrite only the last statement.
    val lastExpandedStmt = expandedStmts.last
    val fusedStmts = expandedStmts.updated(expandedStmts.indexOf(lastExpandedStmt), fuser.rewrite(lastExpandedStmt))

    analyzer.analyze(fusedStmts)(_)
  }

  def apply(query: String,
            columnIdMapping: Map[ColumnName, String],
            schema: Map[String, SoQLType],
            selectedSecondaryInstanceBase: RequestBuilder,
            fuseMap: Map[String, String] = Map.empty,
            merged: Boolean = true): Result = {

    // TODO: handle complex type fusion and join
    def fakeAdapter(columnIdMapping: Map[ColumnName, String])(analyses: Seq[SoQLAnalysis[ColumnName, SoQLType]]): (Seq[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String]) = {
      (analyses, columnIdMapping.map { case(k, v) => QualifiedColumnName(None, k) -> v })
    }

    val postAnalyze = CompoundTypeFuser(fuseMap) match {
      case x if x.eq(NoopFuser) =>
        analyzeQuery(query, columnIdMapping, selectedSecondaryInstanceBase)
      case fuser: SoQLRewrite =>
        val analyze = analyzeQueryWithCompoundTypeFusion(query, fuser, columnIdMapping, schema)
        analyze andThen fuser.postAnalyze andThen fakeAdapter(columnIdMapping)
    }

    val analyzeMaybeMerge = if (merged) { postAnalyze andThen soqlMerge } else { postAnalyze }
    go(columnIdMapping, schema)(analyzeMaybeMerge)
  }

  private def soqlMerge(analysesAndColumnIdMap: (Seq[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String]))
    : (Seq[SoQLAnalysis[ColumnName, SoQLType]], Map[QualifiedColumnName, String]) = {
    val (analyses, qualColumnIdMap) = analysesAndColumnIdMap
    (SoQLAnalysis.merge(andFn, analyses), qualColumnIdMap)
  }
}

object QueryParser extends Logging {

  sealed abstract class Result

  case class SuccessfulParse(analyses: Seq[SoQLAnalysis[String, SoQLType]]) extends Result

  case class AnalysisError(problem: SoQLException) extends Result

  case class UnknownColumnIds(columnIds: Set[String]) extends Result

  case class RowLimitExceeded(max: BigInt) extends Result

  /**
   * Make schema which is a mapping of column name to datatype
   * by going through the raw schema of column id to datatype map.
   * Ignore columns that exist in truth but missing in secondary.
   * @param columnIdMapping column name to column id map (supposedly from soda fountain)
   * @param rawSchema column id to datatype map like ( xxxx-yyyy -> text, ... ) (supposedly from secondary)
   * @return column name to datatype map like ( field_name -> text, ... )
   */
  def dsContext(columnIdMapping: Map[ColumnName, String],
                rawSchema: Map[String, SoQLType]): DatasetContext[SoQLType] =
    try {
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
}
