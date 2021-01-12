package com.socrata.querycoordinator.fusion

import com.socrata.querycoordinator.QueryRewriter._
import com.socrata.querycoordinator.caching.SoQLAnalysisDepositioner
import com.socrata.querycoordinator.{QueryParser, QueryRewriter, Schema, TestBase}
import com.socrata.querycoordinator.util.Join
import com.socrata.soql.ast.Select
import com.socrata.soql.{BinaryTree, SoQLAnalyzer}
import com.socrata.soql.environment.{ColumnName, TypeName}
import com.socrata.soql.functions._
import com.socrata.soql.parsing.Parser
import com.socrata.soql.typed.{ColumnRef, FunctionCall, NumberLiteral}
import com.socrata.soql.types.{SoQLNumber, SoQLPoint, SoQLText, SoQLType}

import scala.util.parsing.input.NoPosition


class CompoundTypeFuserTest extends TestBase {

  import Join._

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  val rewriter = new QueryRewriter(analyzer)

  /** The raw of the table that we get as part of the secondary /schema call */
  val rawSchema = Map[String, SoQLType](
    "aaaa-aaa1" -> SoQLType.typesByName(TypeName("text")),
    "aaaa-aaa2" -> SoQLType.typesByName(TypeName("text")),
    "aaaa-aab1" -> SoQLType.typesByName(TypeName("point")),
    "aaaa-aab2" -> SoQLType.typesByName(TypeName("text")),
    "aaaa-aab3" -> SoQLType.typesByName(TypeName("text")),
    "aaaa-aab4" -> SoQLType.typesByName(TypeName("text")),
    "aaaa-aab5" -> SoQLType.typesByName(TypeName("text")),
    "aaaa-aab6" -> SoQLType.typesByName(TypeName("text"))
  )

  /** QueryRewriter wants a Schema object to have a little stronger typing, so make one */
  val schema = Schema("NOHASH", rawSchema, "NOPK")

  /** Mapping from column name to column id, that we get from soda fountain with the query.  */
  val columnIdMapping = Map[ColumnName, ColumnId](
    ColumnName("code") -> "aaaa-aaa1",
    ColumnName("make") -> "aaaa-aaa2",
    ColumnName("location") -> "aaaa-aab1",
    ColumnName("location_address") -> "aaaa-aab2",
    ColumnName("location_city") -> "aaaa-aab3",
    ColumnName("location_state") -> "aaaa-aab4",
    ColumnName("location_zip") -> "aaaa-aab5",
    ColumnName("some_date") -> "aaaa-aab6"
  )

  /** The dataset context, used for parsing the query */
  val dsContext = QueryParser.dsContext(columnIdMapping, rawSchema)

  test("expanded location columns are fused into location type") {
    val fuser = CompoundTypeFuser(Map("location" -> "location"))
    val q = "SELECT location WHERE location.latitude = 1.1"
    val parsed = new Parser().binaryTreeSelect(q)
    val rewritten: BinaryTree[Select] = fuser.rewrite(parsed, columnIdMapping, rawSchema)
    val analysis = analyzer.analyzeBinary(rewritten)(toAnalysisContext(dsContext))
    val rewrittenAnalysis = fuser.postAnalyze(analysis)
    rewrittenAnalysis.seq.size should be (1)
    rewrittenAnalysis.seq.foreach { analysis =>
      val da = SoQLAnalysisDepositioner(analysis)
      val args = Seq(ColumnRef(None, ColumnName("location"), SoQLPoint.t)(NoPosition),
                     ColumnRef(None, ColumnName("location_address"), SoQLText.t)(NoPosition),
                     ColumnRef(None, ColumnName("location_city"), SoQLText.t)(NoPosition),
                     ColumnRef(None, ColumnName("location_state"), SoQLText.t)(NoPosition),
                     ColumnRef(None, ColumnName("location_zip"), SoQLText.t)(NoPosition))
      val fc = FunctionCall(SoQLFunctions.Location.monomorphic.get, args, None)(NoPosition, NoPosition)
      val fcPointToLat = FunctionCall(SoQLFunctions.PointToLatitude.monomorphic.get,
                                      Seq(ColumnRef(None, ColumnName("location"), SoQLPoint.t)(NoPosition)), None)(NoPosition, NoPosition)
      val argsEq = Seq(fcPointToLat,
                       NumberLiteral(BigDecimal(1.1), SoQLNumber.t)(NoPosition))
      val eqBindings = SoQLFunctions.Eq.parameters.map {
        case VariableType(name) => name -> SoQLNumber
        case _ => throw new Exception("Unexpected function signature")
      }.toMap

      val eqFn = MonomorphicFunction(SoQLFunctions.Eq, eqBindings)
      val fcEq = FunctionCall(eqFn, argsEq, None)(NoPosition, NoPosition)

      da.selection should be (Map(ColumnName("location") -> fc))
      da.where should be (Some(fcEq))
    }
  }
}
