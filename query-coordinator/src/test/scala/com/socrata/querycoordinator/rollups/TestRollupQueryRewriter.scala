package com.socrata.querycoordinator.rollups

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonUtil}
import com.socrata.querycoordinator.QueryRewriter.{ColumnId, RollupName}
import com.socrata.querycoordinator._
import com.socrata.soql._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.parsing.{AbstractParser, Parser}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.{SoQLType, SoQLValue}

import scala.io.Source


case class TestCase(query: String, rewrites: Map[String, String])
object TestCase {
  implicit val jCodec = AutomaticJsonCodecBuilder[TestCase]
}

case class TestConfig(schemas: Map[String, Map[String, (SoQLType, String)]], rollups: Map[String, String], tests: Seq[TestCase])
object TestConfig {
  implicit val jCodec = AutomaticJsonCodecBuilder[TestConfig]
}


class TestRollupQueryRewriter extends TestBase {

  //
  // System Under Test
  //
  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  val rewriter = new QueryRewriterWithJoinEnabled(analyzer)

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
    QueryParser.remapAnalyses(columnMapping, analyses)
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
    val columnMap = columnMapping ++ rollupColumnIds(ruQuery, context, columnMapping)
    QueryParser.remapAnalyses(columnMap, result)
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
    val resource = Source.fromResource(configFile)
    val config = JsonUtil.readJson[TestConfig](resource.reader()).right.get

    val allDatasetContext = AnalysisContext[SoQLType, SoQLValue](
      schemas = config.schemas.mapValues { schemaWithFieldName =>
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

    val allQualifiedColumnNameToIColumnId: Map[QualifiedColumnName, String] = config.schemas.foldLeft(Map.empty[QualifiedColumnName, String]) { (acc, entry) =>
      val (tableName, schemaWithFieldName) = entry
      val map = schemaWithFieldName.map {
        case (iColumnId, (_typ, fieldName)) =>
          val qual = if (tableName == "_") None else Some(tableName)
          QualifiedColumnName(qual, ColumnName(fieldName)) -> iColumnId
      }
      acc ++ map
    }

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

  test("rollup rewriter tests") {
    loadAndRunTests("rollups/query_rewriter_test_configs/test_set_1.json")
  }

  test("rollup rewriter dates tests") {
    loadAndRunTests("rollups/query_rewriter_test_configs/test_dates_1.json")
  }

  //
  // ToDo: rollup rewriter works, but rewrited query analyzer don't
  //
  //  test("rollup rewriter tests 2") {
  //    loadAndRunTests("rollups/query_rewriter_test_configs/test_set_2.json")
  //  }

}