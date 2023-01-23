package com.socrata.querycoordinator.rollups

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import QueryRewriter.{ColumnId, RollupName, Expr}
import com.socrata.querycoordinator._
import com.socrata.soql._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo, SoQLFunctions}
import com.socrata.soql.parsing.{AbstractParser, Parser}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.{SoQLType, SoQLValue}


class TestExpressionRewriter extends BaseConfigurableRollupTest {

  case class TestCase(expression: String, rewrite: Option[String])
  object TestCase {
    implicit val jCodec = AutomaticJsonCodecBuilder[TestCase]
  }

  case class TestConfig(schemas: Map[String, SchemaConfig], rollup: String, tests: Seq[TestCase])
  object TestConfig {
    implicit val jCodec = AutomaticJsonCodecBuilder[TestConfig]
  }

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  private val queryRewriter = new QueryRewriterWithJoinEnabled(analyzer)

  val rewriter = queryRewriter.rewriteExpr


  private def analyzeQuery(q: String,
                           context: AnalysisContext[SoQLType, SoQLValue],
                           columnMapping: Map[QualifiedColumnName, String]):
  SoQLAnalysis[ColumnId, SoQLType] = {

    val parserParams = AbstractParser.Parameters(allowJoins = true)
    val parsed = new Parser(parserParams).binaryTreeSelect(q)
    val analyses = analyzer.analyzeBinary(parsed)(context)
    val merged = SoQLAnalysis.merge(SoQLFunctions.And.monomorphic.get, analyses)
    QueryParser.remapAnalyses(columnMapping, merged).outputSchema.leaf
  }

  private def parseExpression(e: String): Expr = {
    val parserParams = AbstractParser.Parameters(allowJoins = true)
    val parsed = new Parser(parserParams).binaryTreeSelect(q)

  }

  private def loadAndRunTests(configFile: String) {
    val config = getConfig[TestConfig](configFile)
    val allDatasetContext = getContext(config.schemas)
    val allQualifiedColumnNameToIColumnId = getColumnMapping(config.schemas)

    val rollupAnalysis = analyzeQuery(config.rollup, allDatasetContext, allQualifiedColumnNameToIColumnId)
    val rollupColIdx = rollupAnalysis.selection.values.zipWithIndex.toMap

    config.tests.foreach(test => {
      val expression = parseExpression(test.expression)
      val rewrite = rewriter(expression, rollupAnalysis, rollupColIdx)

      val x = rewrite
    })
  }

}
