package com.socrata.querycoordinator

import com.socrata.http.client.RequestBuilder
import com.socrata.querycoordinator.QueryParser.{AnalysisError, SuccessfulParse}
import com.socrata.querycoordinator.caching.SoQLAnalysisDepositioner
import com.socrata.querycoordinator.util.Join
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions._
import com.socrata.soql.parsing.SoQLPosition
import com.socrata.soql.{Leaf, SoQLAnalyzer}
import com.socrata.soql.typed.{ColumnRef, FunctionCall, StringLiteral}
import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLFloatingTimestamp, SoQLText, SoQLType}
import com.socrata.soql.stdlib.Context
import com.rojoma.json.v3.util.JsonUtil

import scala.util.parsing.input.NoPosition

class QueryParserTest extends TestBase {
  import QueryParserTest._ // scalastyle:ignore import.grouping
  import Join.NoQualifier

  test("SELECT * expands all columns") {
    val query = "select *"
    val starPos = query.indexOf("*") + 1
    val expected = com.socrata.soql.collection.OrderedMap(
      ColumnName("a") -> ColumnRef(NoQualifier, "ai", SoQLText)(new SoQLPosition(1, starPos, query, 0)),
      ColumnName("b") -> ColumnRef(NoQualifier, "bi", SoQLText)(new SoQLPosition(1, starPos, query, 0)),
      ColumnName("d") -> ColumnRef(NoQualifier, "di", SoQLFloatingTimestamp)(new SoQLPosition(1, starPos, query, 0)),
      ColumnName("dz") -> ColumnRef(NoQualifier, "dzi", SoQLFixedTimestamp)(new SoQLPosition(1, starPos, query, 0))
    )
    val actual = qp.apply(query, truthColumns, upToDateSchema, fakeRequestBuilder) match {
      case SuccessfulParse(analyses, _) => analyses.asLeaf.get.selection
      case x: QueryParser.Result => x
    }
    actual should be(expected)
  }

  test("SELECT * ignores missing columns") {
    val query = "select *"
    val starPos = query.indexOf("*") + 1
    val expected = com.socrata.soql.collection.OrderedMap(
      ColumnName("a") -> ColumnRef(NoQualifier, "ai", SoQLText)(new SoQLPosition(1, starPos, query, 0))
    )
    val actual = qp.apply(query, truthColumns, outdatedSchema, fakeRequestBuilder) match {
      case SuccessfulParse(analyses, _) => analyses.asLeaf.get.selection
      case x: QueryParser.Result => x
    }
    actual should be(expected)
  }

  test("SELECT param succeeds") {
    val contextStr = """{"u": {"n":{"height_limit.aaaa-bbbb":72}}}"""
    val context = JsonUtil.parseJson[Context](contextStr).right.get
    val query = "select param(@aaaa-bbbb, 'height_limit')"
    val actual = qp.apply(query, truthColumns, outdatedSchema, fakeRequestBuilder, context, Some("aaaa-bbbb"))
    actual shouldBe a[SuccessfulParse]
  }

  test("SELECT param missing from context fails") {
    val query = "select param(@aaaa-bbbb, 'height_limit')"
    val actual = qp.apply(query, truthColumns, outdatedSchema, fakeRequestBuilder, Context.empty, Some("aaaa-bbbb"))
    actual.getClass should be(classOf[AnalysisError])
  }

  test("SELECT param without a lens uid fails") {
    val query = "select param(@aaaa-bbbb, 'height_limit')"
    val contextStr = """{"u": {"n":{"height_limit.aaaa-bbbb":72}}}"""
    val context = JsonUtil.parseJson[Context](contextStr).right.get
    val actual = qp.apply(query, truthColumns, outdatedSchema, fakeRequestBuilder, context, None)
    actual.getClass should be(classOf[QueryParser.ParameterSpecError])
  }

  test("SELECT non existing column errs") {
    val query = "select b"
    val actual = qp.apply(query, truthColumns, outdatedSchema, fakeRequestBuilder)
    actual.getClass should be(classOf[AnalysisError])
  }

  test("Chain Soql") {
    val query = "SELECT a || 'one' as x WHERE a <> 'x' |> SELECT x || 'y' as y where x <> 'y' |> SELECT y || 'z' as z where y <> 'z'"
    val actual = qp.apply(query, truthColumns, outdatedSchema, fakeRequestBuilder, merged = false)
    actual shouldBe a[SuccessfulParse]

    val SuccessfulParse(analyses, _) = actual
    val depositionedAnalyses = analyses.flatMap { a => Leaf(SoQLAnalysisDepositioner(a))}.seq

    val concatBindings = SoQLFunctions.Concat.parameters.map {
      case VariableType(name) => name -> SoQLText
      case _ => throw new Exception("Unexpected function signature")
    }.toMap
    val concat = MonomorphicFunction(SoQLFunctions.Concat, concatBindings)

    val neqBindings = SoQLFunctions.Neq.parameters.map {
      case VariableType(name) => name -> SoQLText
      case _ => throw new Exception("Unexpected function signature")
    }.toMap
    val neq = MonomorphicFunction(SoQLFunctions.Neq, neqBindings)

    val select0 = OrderedMap(ColumnName("x") -> FunctionCall(concat, Seq(
      ColumnRef(NoQualifier, "ai", SoQLText.t)(NoPosition),
      StringLiteral("one", SoQLText.t)(NoPosition)
    ), None, None)(NoPosition, NoPosition))

    val where0 = FunctionCall(neq, Seq(
      ColumnRef(NoQualifier, "ai", SoQLText.t)(NoPosition),
      StringLiteral("x", SoQLText.t)(NoPosition)
    ), None, None)(NoPosition, NoPosition)

    depositionedAnalyses(0).selection should be(select0)
    depositionedAnalyses(0).where should be(Some(where0))

    val select1 = OrderedMap(ColumnName("y") -> FunctionCall(concat, Seq(
      ColumnRef(NoQualifier, "x", SoQLText.t)(NoPosition),
      StringLiteral("y", SoQLText.t)(NoPosition)
    ), None, None)(NoPosition, NoPosition))

    val where1 = FunctionCall(neq, Seq(
      ColumnRef(NoQualifier, "x", SoQLText.t)(NoPosition),
      StringLiteral("y", SoQLText.t)(NoPosition)
    ), None, None)(NoPosition, NoPosition)

    depositionedAnalyses(1).selection should be(select1)
    depositionedAnalyses(1).where should be(Some(where1))

    val select2 = OrderedMap(ColumnName("z") -> FunctionCall(concat, Seq(
      ColumnRef(NoQualifier, "y", SoQLText.t)(NoPosition),
      StringLiteral("z", SoQLText.t)(NoPosition)
    ), None, None)(NoPosition, NoPosition))

    val where2 = FunctionCall(neq, Seq(
      ColumnRef(NoQualifier, "y", SoQLText.t)(NoPosition),
      StringLiteral("z", SoQLText.t)(NoPosition)
    ), None, None)(NoPosition, NoPosition)

    depositionedAnalyses(2).selection should be(select2)
    depositionedAnalyses(2).where should be(Some(where2))
  }

  test("Chain Soql hides not selected columns") {
    val query = "SELECT * |> SELECT a"
    val actual = qp.apply(query, truthColumns, upToDateSchema, fakeRequestBuilder)
    actual shouldBe a[SuccessfulParse]

    val badQuery = "SELECT 'x' as x |> SELECT a"
    val badActual = qp.apply(badQuery, truthColumns, outdatedSchema, fakeRequestBuilder)
    badActual shouldBe a[AnalysisError]
  }
}

object QueryParserTest {

  val defaultRowLimit = 20

  val maxRowLimit = 200

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  val qp = new QueryParser(analyzer, FakeSchemaFetcher, Some(maxRowLimit), defaultRowLimit)

  val truthColumns = Map[ColumnName, String](ColumnName("a") -> "ai", ColumnName("b") -> "bi",
    ColumnName("d") -> "di", ColumnName("dz") -> "dzi")

  val upToDateSchema = Map[String, SoQLType]("ai" -> SoQLText, "bi" -> SoQLText,
    "di" -> SoQLFloatingTimestamp, "dzi" -> SoQLFixedTimestamp)

  val outdatedSchema = Map[String, SoQLType]("ai" -> SoQLText) // Does not have "column bi"

}
