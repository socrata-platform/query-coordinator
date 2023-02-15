package com.socrata.querycoordinator.rollups

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import QueryRewriter.{ColumnId, RollupName}
import com.socrata.querycoordinator._
import com.socrata.soql._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo, SoQLFunctions}
import com.socrata.soql.parsing.{AbstractParser, Parser}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.{SoQLType, SoQLValue}


/*
 * Test configuration file format:
 *
 * {
 *    "schemas": {
 *      "<dataset id>": {
 *        "<column fxf>": ["<column type>", "<column name>"]
 *      },
 *    "rollups": {
 *      "<rollup name>": "<soql query>"
 *    },
 *    "tests": [
 *      {
 *        "query": "<soql query>",
 *        "rewrites": [
 *          "<rollup name>": "<rewritten soql query>"
 *        ]
 *      }
 *    ]
 * }
 *
 *    <dataset id> is "_" for the "root" dataset, and the fxf for the secondary (joined) datasets
 *    multiple rollups could be included, each must have a different name
 *    multiple tests could be included
 *    multiple rewrites could be produced by the rewriter for each relevant rollup
 *
 * To get the schema of the "real" dataset (fake column fxf will be generated as they are not returned by this API):
 *
 * curl /api/views/<fxf>.json | \
 * jq '{(.id):.columns|map({("a"+((100+.id%97)|tostring)+"-"+((1000+.id%997)|tostring)):[.dataTypeName,.name]})|add}'
 */


class TestRollupQueryRewriter extends BaseConfigurableRollupTest {

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
  val rewriter: QueryRewriter = new QueryRewriterWithJoinEnabled(analyzer)

  //
  //  Helper methods
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

  private def analyzeRewrittenQuery(q: String,
                            ruQuery: String,
                            context: AnalysisContext[SoQLType, SoQLValue],
                            columnMapping: Map[QualifiedColumnName, String]):
  BinaryTree[SoQLAnalysis[ColumnId, SoQLType]] = {

    val parserParams =
      AbstractParser.Parameters(
        allowJoins = true
      )
    val parsed = new Parser(parserParams).binaryTreeSelect(q)
    val ruCtx = rollupContext(ruQuery, context, columnMapping)
    val ctx = context.withUpdatedSchemas(_ ++ ruCtx)
    val result = analyzer.analyzeBinary(parsed)(ctx)
    val merged = SoQLAnalysis.merge(SoQLFunctions.And.monomorphic.get, result)
    val columnMap = columnMapping ++ rollupColumnIds(ruQuery, context, columnMapping)
    QueryParser.remapAnalyses(columnMap, merged)
  }

  private def rollupColumnIds(ruQuery: String,
                      context: AnalysisContext[SoQLType, SoQLValue],
                      columnMapping: Map[QualifiedColumnName, String]): Map[QualifiedColumnName, String] = {
    rollupSchema(ruQuery, context, columnMapping).map {
      case (cn, _typ) =>
        QualifiedColumnName(None, cn) -> cn.name
    }
  }

  private def rollupContext(ruQuery: String,
                    context: AnalysisContext[SoQLType, SoQLValue],
                    columnMapping: Map[QualifiedColumnName, String]): Map[String, DatasetContext[SoQLType]] = {
    val map = rollupSchema(ruQuery, context, columnMapping)
    Map("_" -> new DatasetContext[SoQLType] {
      val schema = OrderedMap[ColumnName, SoQLType](map.toSeq: _*)
    })
  }

  private def rollupSchema(ruQuery: String,
                   context: AnalysisContext[SoQLType, SoQLValue],
                   columnMapping: Map[QualifiedColumnName, String]): Map[ColumnName, SoQLType] = {
    val ruAnalyses = analyzeQuery(ruQuery, context, columnMapping).outputSchema.leaf
    ruAnalyses.selection.values.zipWithIndex.map {
      case (expr: CoreExpr[_, _], idx) =>
        (ColumnName(s"c${idx + 1}"), expr.typ.t)
    }.toMap
  }

  //
  //  Main test method
  //
  private def loadAndRunTests(configFile: String) {
    val config = getConfig[TestConfig](configFile)
    val allDatasetContext = getContext(config.schemas)
    val allQualifiedColumnNameToIColumnId = getColumnMapping(config.schemas)

    val rollupAnalysis = QueryRewriter.mergeRollupsAnalysis(
      config.rollups.map {
        case (ruName, ruSoql) =>
          val x = new RollupName(ruName)
          (x, analyzeQuery(ruSoql, allDatasetContext, allQualifiedColumnNameToIColumnId))
      }
    )

    config.tests.foreach(test => {
      val queryAnalysis = analyzeQuery(test.query, allDatasetContext, allQualifiedColumnNameToIColumnId).outputSchema.leaf
      val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

      rewrites should have size test.rewrites.size

      test.rewrites.foreach { case (rollupName, rewrittenQuery) =>
        val rewrittenQueryAnalysis = analyzeRewrittenQuery(
          rewrittenQuery, config.rollups(rollupName), allDatasetContext, allQualifiedColumnNameToIColumnId
        ).outputSchema.leaf

        rewrites should contain key rollupName
        rewrites.get(rollupName).get should equal(rewrittenQueryAnalysis)
      }

    })
  }

  //
  //  Tests
  //

  /*
   *  Tests from TestQueryRewriter
   */
  test("rollup rewriter tests") {
    loadAndRunTests("rollups/query_rewriter_test_configs/test_query_rewriter.json")
  }

  test("rollup rewriter union rollup") {
    loadAndRunTests("rollups/query_rewriter_test_configs/test_union.json")
  }


  /*
   * Tests from TestQueryRewriterDateTrunc, TestQueryRewriterDateTruncLtGte
   */
  test("rollup rewriter dates tests date trunc") {
    loadAndRunTests("rollups/query_rewriter_test_configs/test_date_trunc.json")
  }

  test("rollup rewriter dates tests date trunc ltgte") {
    loadAndRunTests("rollups/query_rewriter_test_configs/test_date_trunc_ltgte.json")
  }
  /*
   * Piped rollups and queries tests
   */
  test("piped rollup queries") {
    loadAndRunTests("rollups/query_rewriter_test_configs/test_pipes_1.json")
  }

  test("piped rollup queries 2") {
    loadAndRunTests("rollups/query_rewriter_test_configs/test_pipes_2.json")
  }

  /*
   * Grouped rollups and queries tests
   */
  test("group by in rollup") {
    loadAndRunTests("rollups/query_rewriter_test_configs/test_group_by.json")
  }

  /*
   * Partitions
   */
  test("partitions in rollups") {
    loadAndRunTests("rollups/query_rewriter_test_configs/test_partitions.json")
  }

  test("self aggregatable aggregate") {
    loadAndRunTests("rollups/query_rewriter_test_configs/test_self_aggregatable_aggregate.json")
  }

  test("derived view join") {
    loadAndRunTests("rollups/query_rewriter_test_configs/test_derived_view_join.json")
  }

}
