package com.socrata.querycoordinator.rollups

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import QueryRewriter.{Expr}
import com.socrata.querycoordinator._
import com.socrata.soql._
import com.socrata.soql.environment.{ColumnName}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLFunctions, SoQLTypeInfo}
import com.socrata.soql.parsing.{AbstractParser, Parser}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.typechecker.Typechecker

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

  val expressionRewriter = queryRewriter.rewriteExpr


  private def analyzeRollup(q: String,
                           context: AnalysisContext[SoQLType, SoQLValue],
                           columnMapping: Map[QualifiedColumnName, String]):
  BinaryTree[SoQLAnalysis[ColumnName, SoQLType]] = {

    val parserParams = AbstractParser.Parameters(allowJoins = true)
    val parsed = new Parser(parserParams).binaryTreeSelect(q)
    val analyses = analyzer.analyzeBinary(parsed)(context)
    val merged = SoQLAnalysis.merge(SoQLFunctions.And.monomorphic.get, analyses)
    merged
  }

  private def analyzeExpression(e: String,
                                context: AnalysisContext[SoQLType, SoQLValue],
                                aliases: Map[ColumnName, CoreExpr[ColumnName, SoQLType]],
                                columnMapping: Map[QualifiedColumnName, String]): Expr = {

    val typechecker = new Typechecker(SoQLTypeInfo, SoQLFunctionInfo)(context)

    val parserParams = AbstractParser.Parameters(allowJoins = true)
    val expression = new Parser(parserParams).expression(e)
    val analysis = typechecker(expression, aliases, None)
    analysis.mapColumnIds((n, t) => columnMapping(QualifiedColumnName(t, n)))
  }

  private def loadAndRunTests(configFile: String) {
    val config = getConfig[TestConfig](configFile)
    val allDatasetContext = getContext(config.schemas)
    val allQualifiedColumnNameToIColumnId = getColumnMapping(config.schemas)

    val rollupAnalysisTree = analyzeRollup(config.rollup, allDatasetContext, allQualifiedColumnNameToIColumnId)
    val rollupAnalysis = rollupAnalysisTree.outputSchema.leaf
    val remappedAnalysis = QueryParser.remapAnalyses(allQualifiedColumnNameToIColumnId, rollupAnalysisTree).outputSchema.leaf
    val rollupColIdx = remappedAnalysis.selection.values.zipWithIndex.toMap

    config.tests.foreach(test => {
      val expression = analyzeExpression(test.expression,
        allDatasetContext,
        rollupAnalysis.selection,
        allQualifiedColumnNameToIColumnId
      )

      val rewrite = expressionRewriter(expression, remappedAnalysis, rollupColIdx, isInAggregate = false)

      rewrite match {
        case Some(expr) => test.rewrite should equal(Some(expr.toString()))
        case None => test.rewrite should equal(None)
      }
    })
  }

  test("rewrite expression") {
    loadAndRunTests("rollups/expression_rewriter_test_configs/test_dates.json")
  }

}
