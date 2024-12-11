package com.socrata.querycoordinator.fusion

import scala.util.parsing.input.NoPosition
import com.socrata.querycoordinator.{FakeSchemaFetcher, QueryParser, TestBase}
import com.socrata.querycoordinator.QueryParser.SuccessfulParse
import com.socrata.querycoordinator.caching.SoQLAnalysisDepositioner
import com.socrata.querycoordinator.util.Join.NoQualifier
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions._
import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.soql.stdlib.Context

class QueryParserFuseTest extends TestBase {
  import QueryParserFuseTest._ // scalastyle:ignore import.grouping

  test("SELECT * -- compound type fusing") {
    val query = "SELECT * WHERE location.latitude = 1.1"

    val expectedWhere: CoreExpr[String, SoQLType] =
      FunctionCall(eqFn, Seq(ptlFc, NumberLiteral(1.1, SoQLNumber.t)(NoPosition)), None, None)(NoPosition, NoPosition)

    val expectedSelection = com.socrata.soql.collection.OrderedMap(
      ColumnName("a") -> ColumnRef(NoQualifier, "ai", SoQLText)(NoPosition),
      ColumnName("b") -> ColumnRef(NoQualifier, "bi", SoQLText)(NoPosition),
      ColumnName("location") -> locationFc,
      ColumnName("url") -> urlFc
    )


    val actual = qp.apply(query, truthColumns, schema, fakeRequestBuilder, fuseMap = fuse) match {
      case SuccessfulParse(analyses, _) =>
        val actual = SoQLAnalysisDepositioner(analyses.asLeaf.get)
        actual.selection should be(expectedSelection)
        actual.where should be(Some(expectedWhere))
      case x: QueryParser.Result =>
        fail("fail to parse soql: " + x)
    }
  }

  test("SELECT * |> SELECT url, url_description") {
    val query = "SELECT * |> SELECT url, url_description"

    val expectedSelection = com.socrata.soql.collection.OrderedMap(
      ColumnName("url") -> urlFc
    )

    val actual = qp.apply(query, truthColumns, schema, fakeRequestBuilder, fuseMap = fuse) match {
      case SuccessfulParse(analyses, _) =>
        val actual = SoQLAnalysisDepositioner(analyses.seq.head)
        actual.selection should be(expectedSelection)
        actual.where should be(None)
      case x: QueryParser.Result =>
        fail("fail to parse soql: " + x)
    }
  }

  test("SELECT url, url_description |> SELECT * LIMIT 100000000") {
    val query = "SELECT url, url_description, a |> SELECT * LIMIT 100000000"

    val expectedSelection = com.socrata.soql.collection.OrderedMap(
      ColumnName("url") -> urlFc,
      ColumnName("a") -> ColumnRef(NoQualifier, "ai", SoQLText)(NoPosition)
    )

    val actual = qp.apply(query, truthColumns, schema, fakeRequestBuilder, fuseMap = fuse) match {
      case SuccessfulParse(analyses, _) =>
        val actual = SoQLAnalysisDepositioner(analyses.seq.head)
        actual.selection should be(expectedSelection)
        actual.where should be(None)
        actual.limit should be(Some(BigInt(100000000)))
      case x: QueryParser.Result =>
        fail("fail to parse soql: " + x)
    }
  }

  test("SELECT :*, * | SELECT url, url_description, :id -- no fusing") {
    val query = "SELECT :*, * |> SELECT url, url_description, :id"

    val expectedSelection = com.socrata.soql.collection.OrderedMap(
      ColumnName("url") -> ColumnRef(NoQualifier, "url", SoQLText)(NoPosition),
      ColumnName("url_description") -> ColumnRef(NoQualifier, "url_description", SoQLText)(NoPosition),
      ColumnName(":id") -> ColumnRef(NoQualifier, ":id", SoQLID)(NoPosition)
    )

    val actual = qp.apply(query, truthColumns, schema, fakeRequestBuilder, fuseMap = Map.empty) match {
      case SuccessfulParse(analyses, _) =>
        val actual = SoQLAnalysisDepositioner(analyses.seq.head)
        actual.selection should be(expectedSelection)
        actual.where should be(None)
      case x: QueryParser.Result =>
        fail("fail to parse soql: " + x)
    }
  }

  test("SELECT a -- non existent fuse type is ignored") {
    val query = "SELECT a"

    val expectedSelection = com.socrata.soql.collection.OrderedMap(
      ColumnName("a") -> ColumnRef(NoQualifier, "ai", SoQLText)(NoPosition)
    )

    val actual = qp.apply(query, truthColumns, schema, fakeRequestBuilder, fuseMap = Map.empty) match {
      case SuccessfulParse(analyses, _) =>
        val actual = SoQLAnalysisDepositioner(analyses.seq.head)
      case x: QueryParser.Result =>
        fail("fail to parse soql: " + x)
    }
  }
}

object QueryParserFuseTest {

  val defaultRowLimit = 20

  val maxRowLimit = 200000000

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  val qp = new QueryParser(analyzer, FakeSchemaFetcher, Some(maxRowLimit), defaultRowLimit)

  val truthColumns = Map[ColumnName, String](
    ColumnName(":id") -> ":id",
    ColumnName("a") -> "ai",
    ColumnName("b") -> "bi",
    ColumnName("location") -> "location",
    ColumnName("location_address") -> "location_address",
    ColumnName("location_city") -> "location_city",
    ColumnName("location_state") -> "location_state",
    ColumnName("location_zip") -> "location_zip",
    ColumnName("url") -> "url",
    ColumnName("url_description") -> "url_description"
  )

  val schema = Map[String, SoQLType](
    ":id" -> SoQLID,
    "ai" -> SoQLText,
    "bi" -> SoQLText,
    "location" -> SoQLPoint,
    "location_address" -> SoQLText,
    "location_city" -> SoQLText,
    "location_state" -> SoQLText,
    "location_zip" -> SoQLText,
    "url" -> SoQLText,
    "url_description" -> SoQLText
  )

  val locationFc = FunctionCall(SoQLFunctions.Location.monomorphic.get, Seq(
    ColumnRef(NoQualifier, "location", SoQLPoint.t)(NoPosition),
    ColumnRef(NoQualifier, "location_address", SoQLText.t)(NoPosition),
    ColumnRef(NoQualifier, "location_city", SoQLText.t)(NoPosition),
    ColumnRef(NoQualifier, "location_state", SoQLText.t)(NoPosition),
    ColumnRef(NoQualifier, "location_zip", SoQLText.t)(NoPosition)
  ), None, None)(NoPosition, NoPosition)

  val urlFc = FunctionCall(SoQLFunctions.Url.monomorphic.get, Seq(
    ColumnRef(NoQualifier, "url", SoQLText.t)(NoPosition),
    ColumnRef(NoQualifier, "url_description", SoQLText.t)(NoPosition)
  ), None, None)(NoPosition, NoPosition)

  val eqBindings = SoQLFunctions.Eq.parameters.map {
    case VariableType(name) => name -> SoQLNumber
    case _ => throw new Exception("Unexpected function signature")
  }.toMap

  val eqFn = MonomorphicFunction(SoQLFunctions.Eq, eqBindings)

  val ptlFc = FunctionCall(SoQLFunctions.PointToLatitude.monomorphic.get,
    Seq(ColumnRef(NoQualifier, "location", SoQLPoint.t)(NoPosition)), None, None)(NoPosition, NoPosition)

  val fuse = Map("location" -> "location", "url" -> "url")
}
