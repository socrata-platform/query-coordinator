package com.socrata.querycoordinator.fusion

import scala.util.parsing.input.NoPosition
import com.socrata.querycoordinator._
import com.socrata.querycoordinator.QueryParser.SuccessfulParse
import com.socrata.querycoordinator.SchemaFetcher.SuccessfulExtendedSchema
import com.socrata.querycoordinator.caching.SoQLAnalysisDepositioner
import com.socrata.querycoordinator.util.Join.NoQualifier
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions._
import com.socrata.soql.typed._
import com.socrata.soql.types._
import org.joda.time.DateTime

class QueryParserFuseTest extends TestBase {
  import QueryParserFuseTest._ // scalastyle:ignore import.grouping

  test("SELECT * -- compound type fusing") {
    val query = "SELECT * WHERE location.latitude = 1.1"

    val expectedWhere: CoreExpr[String, SoQLType] =
      FunctionCall(eqFn, Seq(ptlFc, NumberLiteral(1.1, SoQLNumber.t)(NoPosition)))(NoPosition, NoPosition)

    val expectedSelection = com.socrata.soql.collection.OrderedMap(
      ColumnName("a") -> ColumnRef(NoQualifier, "ai", SoQLText)(NoPosition),
      ColumnName("b") -> ColumnRef(NoQualifier, "bi", SoQLText)(NoPosition),
      ColumnName("location") -> locationFc,
      ColumnName("phone") -> phoneFc
    )

    val actual = qp.apply(datasetId, query, schema, fakeRequestBuilder, fuse) match {
      case SuccessfulParse(analyses) =>
        val actual = SoQLAnalysisDepositioner(analyses.head)
        actual.selection should be(expectedSelection)
        actual.where should be(Some(expectedWhere))
      case x: QueryParser.Result =>
        fail("fail to parse soql: " + x)
    }
  }

  test("SELECT * |> SELECT phone, phone_type") {
    val query = "SELECT * |> SELECT phone, phone_type"

    val expectedSelection = com.socrata.soql.collection.OrderedMap(
      ColumnName("phone") -> phoneFc
    )

    val actual = qp.apply(datasetId, query, schema, fakeRequestBuilder, fuse) match {
      case SuccessfulParse(analyses) =>
        val actual = SoQLAnalysisDepositioner(analyses.head)
        actual.selection should be(expectedSelection)
        actual.where should be(None)
      case x: QueryParser.Result =>
        fail("fail to parse soql: " + x)
    }
  }

  test("SELECT phone, phone_type |> SELECT * LIMIT 100000000") {
    val query = "SELECT phone, phone_type, a |> SELECT * LIMIT 100000000"

    val expectedSelection = com.socrata.soql.collection.OrderedMap(
      ColumnName("phone") -> phoneFc,
      ColumnName("a") -> ColumnRef(NoQualifier, "ai", SoQLText)(NoPosition)
    )

    val actual = qp.apply(datasetId, query, schema, fakeRequestBuilder, fuse) match {
      case SuccessfulParse(analyses) =>
        val actual = SoQLAnalysisDepositioner(analyses.head)
        actual.selection should be(expectedSelection)
        actual.where should be(None)
        actual.limit should be(Some(BigInt(100000000)))
      case x: QueryParser.Result =>
        fail("fail to parse soql: " + x)
    }
  }

  test("SELECT :*, * | SELECT phone, phone_type, :id -- no fusing") {
    val query = "SELECT :*, * |> SELECT phone, phone_type, :id"

    val expectedSelection = com.socrata.soql.collection.OrderedMap(
      ColumnName("phone") -> ColumnRef(NoQualifier, "phone", SoQLText)(NoPosition),
      ColumnName("phone_type") -> ColumnRef(NoQualifier, "phone_type", SoQLText)(NoPosition),
      ColumnName(":id") -> ColumnRef(NoQualifier, ":id", SoQLID)(NoPosition)
    )

    val actual = qp.apply(datasetId, query, schema, fakeRequestBuilder, Map.empty) match {
      case SuccessfulParse(analyses) =>
        val actual = SoQLAnalysisDepositioner(analyses.head)
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

    val actual = qp.apply(datasetId, query, schema, fakeRequestBuilder, Map.empty) match {
      case SuccessfulParse(analyses) =>
        val actual = SoQLAnalysisDepositioner(analyses.head)
      case x: QueryParser.Result =>
        fail("fail to parse soql: " + x)
    }
  }
}

object QueryParserFuseTest {

  val defaultRowLimit = 20

  val maxRowLimit = 200000000

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  val truthSchema = Map[String, Tuple2[SoQLType, String]](
    ":id" -> Tuple2(SoQLID, ":id"),
    "ai" -> Tuple2(SoQLText, "a"),
    "bi" -> Tuple2(SoQLText, "b"),
    "location" -> Tuple2(SoQLPoint, "location"),
    "location_address" -> Tuple2(SoQLText, "location_address"),
    "location_city" -> Tuple2(SoQLText, "location_city"),
    "location_state" -> Tuple2(SoQLText, "location_state"),
    "location_zip" -> Tuple2(SoQLText, "location_zip"),
    "phone" -> Tuple2(SoQLText, "phone"),
    "phone_type" -> Tuple2(SoQLText, "phone_type"))

  val schemaWithFieldName = SchemaWithFieldName(truthSchema.hashCode().toString, truthSchema, pk = ":id")
  val extendedSchema = SuccessfulExtendedSchema(schemaWithFieldName, 0, 0, new DateTime(0))

  val qp = new QueryParser(analyzer, new FakeSchemaFetcher(extendedSchema), Some(maxRowLimit), defaultRowLimit)

  val schema: Map[String, SoQLType] = truthSchema.map { case (k, v) => (k -> v._1) }

  val locationFc = FunctionCall(SoQLFunctions.Location.monomorphic.get, Seq(
    ColumnRef(NoQualifier, "location", SoQLPoint.t)(NoPosition),
    ColumnRef(NoQualifier, "location_address", SoQLText.t)(NoPosition),
    ColumnRef(NoQualifier, "location_city", SoQLText.t)(NoPosition),
    ColumnRef(NoQualifier, "location_state", SoQLText.t)(NoPosition),
    ColumnRef(NoQualifier, "location_zip", SoQLText.t)(NoPosition)
  ))(NoPosition, NoPosition)

  val phoneFc = FunctionCall(SoQLFunctions.Phone.monomorphic.get, Seq(
    ColumnRef(NoQualifier, "phone", SoQLText.t)(NoPosition),
    ColumnRef(NoQualifier, "phone_type", SoQLText.t)(NoPosition)
  ))(NoPosition, NoPosition)

  val eqBindings = SoQLFunctions.Eq.parameters.map {
    case VariableType(name) => name -> SoQLNumber
    case _ => throw new Exception("Unexpected function signature")
  }.toMap

  val eqFn = MonomorphicFunction(SoQLFunctions.Eq, eqBindings)

  val ptlFc = FunctionCall(SoQLFunctions.PointToLatitude.monomorphic.get,
    Seq(ColumnRef(NoQualifier, "location", SoQLPoint.t)(NoPosition)))(NoPosition, NoPosition)

  val fuse = Map("location" -> "location", "phone" -> "phone")
}
