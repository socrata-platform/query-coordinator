package com.socrata.querycoordinator.rollups

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.querycoordinator._
import com.socrata.querycoordinator.rollups.QueryRewriter.ColumnId
import com.socrata.soql._
import com.socrata.soql.environment.{TableName}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLFunctions, SoQLTypeInfo}
import com.socrata.soql.parsing.{AbstractParser, Parser}
import com.socrata.soql.types.{SoQLType, SoQLValue}


class QueryRewriterTest extends BaseConfigurableRollupTest {

  case class TestCase(query: String, rewrites: Map[String, String])

  object TestCase {
    implicit val jCodec = AutomaticJsonCodecBuilder[TestCase]
  }

  case class TestConfig(schemas: Map[String, SchemaConfig], rollups: Map[String, String], tests: Seq[TestCase])

  object TestConfig {
    implicit val jCodec = AutomaticJsonCodecBuilder[TestConfig]
  }

  //
  // System Under Test
  //
  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  val rewriter: QueryRewriter = new CompoundQueryRewriter(analyzer)

  //
  //  Fetchers for QueryRewriter
  //
  def fetchRollupInfo(config: TestConfig): Seq[RollupInfo] = config.rollups.map({
    case (k, v) => RollupInfo(k, v)
  }).toList

  def getSchemaByTableName(tableName: TableName, config: TestConfig): SchemaWithFieldName = {
    SchemaWithFieldName(
      tableName.name,
      config.schemas.getOrElse(tableName.name, Map.empty),
      ":pk"
    )
  }

  //
  //  Query loader
  //
  private def analyzeQuery(q: String,
                           context: AnalysisContext[SoQLType, SoQLValue],
                           columnMapping: Map[QualifiedColumnName, String]):
  BinaryTree[SoQLAnalysis[ColumnId, SoQLType]] = {

    val parserParams = AbstractParser.Parameters(allowJoins = true)
    val parsed = new Parser(parserParams).binaryTreeSelect(q)
    val analyses = analyzer.analyzeBinary(parsed)(context)
    val merged = SoQLAnalysis.merge(SoQLFunctions.And.monomorphic.get, analyses)
    QueryParser.remapAnalyses(columnMapping, merged)
  }

  //
  //  Generic method to load and run test
  //
  private def loadAndRunTests(configFile: String) {
    val config = getConfig[TestConfig](configFile)
    val context = getContext(config.schemas)
    val mapping = getColumnMapping(config.schemas)
    val schema = Schema(
      "",
      config.schemas.getOrElse("_", Map.empty).map({ case (k, v) => (k, v._1)}),
      ":pk"
    )

    config.tests.foreach(test => {
      val queryAnalysis = analyzeQuery(test.query, context, mapping)

      val rewrites = rewriter.possiblyRewriteOneAnalysisInQuery(
        "_",
        schema,
        queryAnalysis,
        None,
        () => fetchRollupInfo(config),
        (tableName) => getSchemaByTableName(tableName, config),
        true
      )

      rewrites._2 should have size test.rewrites.size

      test.rewrites.foreach { case (rollupName, rewrittenQuery) =>
        rewrites._1.toString should equal(rewrittenQuery)
      }

    })

  }

  //
  //  Actual test
  //
  test("rollup rewriter tests") {
    loadAndRunTests("rollups/query_rewriter_real_test_configs/test_query_rewriter.json")
  }


}
