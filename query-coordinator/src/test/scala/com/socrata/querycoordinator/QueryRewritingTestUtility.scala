package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryRewriter.{ColumnId, RollupName}
import com.socrata.soql._
import com.socrata.soql.ast.Select
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.{SoQLType, SoQLValue}
import org.scalatest.Matchers.{convertToAnyShouldWrapper, equal}

object QueryRewritingTestUtility {
  // Some type definitions for easier use
  type DatasetDefinitions = Map[String, Map[String, (SoQLType, String)]]
  type AnalyzedDatasetContext = AnalysisContext[SoQLType, SoQLValue]
  type ColumnMappings = Map[QualifiedColumnName, String]
  type ParsedSoql = BinaryTree[Select]
  type AnalyzedSoql = BinaryTree[SoQLAnalysis[ColumnName, SoQLType]]
  type RemappedAnalyzedSoql = BinaryTree[SoQLAnalysis[ColumnId, SoQLType]]
  type SoqlAnalysisFunction = AnalyzedDatasetContext => ParsedSoql => AnalyzedSoql
  type RemapAnalyzedSoqlFunction = ColumnMappings => AnalyzedSoql => RemappedAnalyzedSoql
  type AnalyzeSoqlAndRemapFunction = ParsedSoql => RemappedAnalyzedSoql
  type SoqlParseFunction = String => BinaryTree[Select]
  type RollupsDefinition = Map[String, String]
  type Rewrites = (RemappedAnalyzedSoql, Seq[String])
  type RemappedRollupAnalysis = Map[RollupName, RemappedAnalyzedSoql]
  type QueryDefinition = String
  type RewriteFunction = (RemappedAnalyzedSoql, RemappedRollupAnalysis) => Rewrites
  // Crazy function that helps build what the rewritten query should be
  type BuildRewrittenContextFunction = (RollupsDefinition, SoqlParseFunction, AnalyzedDatasetContext, SoqlAnalysisFunction, ColumnMappings, RemapAnalyzedSoqlFunction) => (RemappedAnalyzedSoql, Seq[String])


  // This is the test method to use to assert query rewriting functionality
  def AssertRewrite(soqlParseFunction: SoqlParseFunction, analysisFunction: SoqlAnalysisFunction, analysisMappingFunction: RemapAnalyzedSoqlFunction, rewriteFunction: RewriteFunction)(datasetDefinitions: DatasetDefinitions, rollupsDefinition: RollupsDefinition, queryDefinition: QueryDefinition, expected: BuildRewrittenContextFunction): Unit = {
    val (datasetContext, columnMappings) = buildDatasetContextAndMapping(datasetDefinitions)
    val rollupAnalysis = buildRollups((a) => analysisMappingFunction(columnMappings)(analysisFunction(datasetContext)(a)), soqlParseFunction)(rollupsDefinition)
    val query = analysisMappingFunction(columnMappings)(analysisFunction(datasetContext)(soqlParseFunction(queryDefinition)))
    val actual: Rewrites = rewriteFunction(query, rollupAnalysis)
    actual should equal(expected(rollupsDefinition, soqlParseFunction, datasetContext, analysisFunction, columnMappings, analysisMappingFunction))

  }

  def buildRollups(anal: AnalyzeSoqlAndRemapFunction, parser: SoqlParseFunction)(rollups: RollupsDefinition): RemappedRollupAnalysis = {
    rollups.map { case (name, soql) => (new RollupName(name), anal(parser(soql))) }
  }

  def buildDatasetContextAndMapping(m: DatasetDefinitions): (AnalyzedDatasetContext, ColumnMappings) = {
    (buildDatasetContext(m), buildColumnMapping(m))
  }

  // We need to be aware of the dataset column identifier to column name mappings
  def buildColumnMapping(m: DatasetDefinitions): ColumnMappings = {
    m.foldLeft(Map.empty[QualifiedColumnName, String]) { (acc, entry) =>
      val (tableName, schemaWithFieldName) = entry
      val map = schemaWithFieldName.map {
        case (iColumnId, (_typ, fieldName)) =>
          val qual = if (tableName == "_") None else Some(tableName)
          QualifiedColumnName(qual, ColumnName(fieldName)) -> iColumnId
      }
      acc ++ map
    }
  }

  def buildDatasetContext(m: DatasetDefinitions): AnalyzedDatasetContext = {
    AnalysisContext[SoQLType, SoQLValue](
      schemas = m.mapValues { schemaWithFieldName =>
        val columnNameToType = schemaWithFieldName.map {
          case (_iColumnId, (typ, fieldName)) =>
            (ColumnName(fieldName), typ)
        }
        new DatasetContext[SoQLType] {
          val schema: OrderedMap[ColumnName, SoQLType] =
            OrderedMap[ColumnName, SoQLType](columnNameToType.toSeq: _*)
        }
      },
      parameters = ParameterSpec.empty
    )
  }

  // This is the function passed as the last argument to AssertRewrite, that helps build what the expected rewritten query should be. Second argument set satisfies BuildRewrittenContextFunction
  def AnalyzeRewrittenFromRollup(expectedRewrittenQuery: String, expectedRollupName: String)(rollupsDefinition: RollupsDefinition, soqlParseFunction: SoqlParseFunction, analyzedDatasetContext: AnalyzedDatasetContext, analysisFunction: SoqlAnalysisFunction, columnMapping: ColumnMappings, analysisMappingFunction: RemapAnalyzedSoqlFunction): (RemappedAnalyzedSoql, Seq[String]) = {
    val rollup = rollupsDefinition(expectedRollupName)
    val parsed = soqlParseFunction(expectedRewrittenQuery)
    val ruCtx = rollupContext(soqlParseFunction, analysisFunction, analyzedDatasetContext)(rollup)
    val ctx = analyzedDatasetContext.withUpdatedSchemas(_ ++ ruCtx)
    val result = analysisFunction(ctx)(parsed)
    val columnMap = columnMapping ++ rollupColumnIds(soqlParseFunction, analysisFunction, analyzedDatasetContext)(rollup)
    (analysisMappingFunction(columnMap)(result), Seq(expectedRollupName))
  }

  def rollupContext(soqlParseFunction: SoqlParseFunction, analysisFunction: SoqlAnalysisFunction, analyzedDatasetContext: AnalyzedDatasetContext)(ruQuery: String): Map[String, DatasetContext[SoQLType]] = {
    val map = rollupSchema(soqlParseFunction, analysisFunction, analyzedDatasetContext)(ruQuery)
    Map("_" -> new DatasetContext[SoQLType] {
      val schema = OrderedMap[ColumnName, SoQLType](map.toSeq: _*)
    })
  }

  def rollupSchema(soqlParseFunction: SoqlParseFunction, analysisFunction: SoqlAnalysisFunction, analyzedDatasetContext: AnalyzedDatasetContext)(ruQuery: String): Map[ColumnName, SoQLType] = {
    val ruAnalyses = analysisFunction(analyzedDatasetContext)(soqlParseFunction(ruQuery)).outputSchema.leaf
    ruAnalyses.selection.values.zipWithIndex.map {
      case (expr: CoreExpr[_, _], idx) =>
        (ColumnName(s"c${idx + 1}"), expr.typ.t)
    }.toMap
  }

  def rollupColumnIds(soqlParseFunction: SoqlParseFunction, analysisFunction: SoqlAnalysisFunction, analyzedDatasetContext: AnalyzedDatasetContext)(ruQuery: String): Map[QualifiedColumnName, String] = {
    rollupSchema(soqlParseFunction, analysisFunction, analyzedDatasetContext)(ruQuery).map {
      case (cn, _typ) =>
        QualifiedColumnName(None, cn) -> cn.name
    }
  }

  def ReMap(mapping: ColumnMappings)(analysis: AnalyzedSoql): RemappedAnalyzedSoql = {
    QueryParser.remapAnalyses(mapping, analysis)
  }

  def Analyze(analyzer: SoQLAnalyzer[SoQLType, SoQLValue])(ctx: AnalyzedDatasetContext)(parsed: ParsedSoql): AnalyzedSoql = {
    analyzer.analyzeBinary(parsed)(ctx)
  }
}
