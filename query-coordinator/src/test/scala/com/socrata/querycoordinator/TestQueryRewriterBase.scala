package com.socrata.querycoordinator

import com.socrata.querycoordinator.rollups.QueryRewriter
import com.socrata.querycoordinator.rollups.QueryRewriter.{Anal, ColumnId, Expr, RollupName}
import com.socrata.querycoordinator.util.Join
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.environment.{ColumnName, TableName, TypeName}
import com.socrata.soql.typed.Qualifier
import com.socrata.soql.types.SoQLType

abstract class TestQueryRewriterBase extends TestBase with TestCompoundQueryRewriterBase {

  import Join._

  /** The raw of the table that we get as part of the secondary /schema call */
  val rawSchema = Map[String, SoQLType](
    "dxyz-num1" -> SoQLType.typesByName(TypeName("number")),
    ":wido-ward" -> SoQLType.typesByName(TypeName("number")),
    "crim-typ3" -> SoQLType.typesByName(TypeName("text")),
    "dont-roll" -> SoQLType.typesByName(TypeName("text")),
    "crim-date" -> SoQLType.typesByName(TypeName("floating_timestamp")),
    "some-date" -> SoQLType.typesByName(TypeName("floating_timestamp"))
  )

  /** QueryRewriter wants a Schema object to have a little stronger typing, so make one */
  val schema = Schema("NOHASH", rawSchema, "NOPK")

  /** Mapping from column name to column id, that we get from soda fountain with the query.  */
  val columnIdMapping = Map[ColumnName, ColumnId](
    ColumnName("number1") -> "dxyz-num1",
    ColumnName("ward") -> ":wido-ward",
    ColumnName("crime_type") -> "crim-typ3",
    ColumnName("dont_create_rollups") -> "dont-roll",
    ColumnName("crime_date") -> "crim-date",
    ColumnName("some_date") -> "some-date"
  )

  /** The dataset context, used for parsing the query */
  val dsContext = QueryParser.dsContext(columnIdMapping, rawSchema)

  val rawSchemaJoin = Map[String, SoQLType](
    "crim-typ3" -> SoQLType.typesByName(TypeName("text")),
    "aaaa-aaaa" -> SoQLType.typesByName(TypeName("text")),
    "bbbb-bbbb" -> SoQLType.typesByName(TypeName("text")),
    "dddd-dddd" -> SoQLType.typesByName(TypeName("floating_timestamp")),
    "nnnn-nnnn" -> SoQLType.typesByName(TypeName("number"))
  )

  val schemaJoin = Schema("NOHASH", rawSchemaJoin, "NOPK")

  val columnIdMappingJoin = Map[ColumnName, ColumnId](
    ColumnName("crime_type") -> "crim-typ3",
    ColumnName("aa") -> "aaaa-aaaa",
    ColumnName("bb") -> "bbbb-bbbb",
    ColumnName("floating") -> "dddd-dddd",
    ColumnName("nn") -> "nnnn-nnnn"
  )

  val dsContextJoin = QueryParser.dsContext(columnIdMappingJoin, rawSchemaJoin)

  val joinTable = TableName("_tttt-tttt", Some("t1"))

  def multiTablesColumnIdMapping(cn: ColumnName, q: Qualifier): ColumnId = {
    q match {
      case None =>
        columnIdMapping(cn)
      case Some(q) if q == TableName.SodaFountainPrefix + joinTable.qualifier =>
        columnIdMappingJoin(cn)
      case Some(unknown) =>
        throw new Exception(s"Unknown qualifier $unknown")
    }
  }

  def getSchemaWithFieldName(tn: TableName): SchemaWithFieldName = {
    tn match {
      case tn: TableName if tn.name == joinTable.name =>
        val schemaWithFieldName = columnIdMappingJoin.map {
          case (cn: ColumnName, cid) =>
            (cid, (rawSchemaJoin(cid), cn.name))
        }
        SchemaWithFieldName("NOHASH", schemaWithFieldName, "NOPK")
      case _ =>
        throw new Exception("table not found")
    }
  }

  override def beforeAll() {
    withClue("Not all rollup definitions successfully parsed, check log for failures") {
      if (rollupAnalyses.isEmpty) {
        rollupAnalysis should have size (rollups.size)
      } else {
        rollupAnalyses should have size (rollups.size)
      }
    }
  }

  /** Analyze the query and map to column ids, just like we have in real life. */
  override def analyzeQuery(q: String): SoQLAnalysis[ColumnId, SoQLType] = {
    val ctx = toAnalysisContext(dsContext).withUpdatedSchemas(_ + (joinTable.nameWithSodaFountainPrefix -> dsContextJoin))
    val analysis = analyzer.analyzeUnchainedQuery(q)(ctx)
    analysis.mapColumnIds(multiTablesColumnIdMapping)
  }

  /** Silly half-assed function for debugging when things don't match */
  def compareProducts(a: Product, b: Product, indent: Int = 0): Unit = {
    val zip = a.productIterator.zip(b.productIterator)
    zip.foreach { case (x, y) =>
      println(">" * indent + "compare:" + (x == y) + " -- " + x + " == " + y)
      (x, y) match {
        case (xx: Product, yy: Product) => compareProducts(xx, yy, indent + 1)
        case _ => println(">" * indent + s"Can't compare ${x} with ${y} but ... ${x.hashCode} vs ${y.hashCode}")
      }
    }
  }

  /**
    * A handy function to test QueryRewriter.rewriteExpr
    */
  def rewriteExpr(rewriter: QueryRewriter, e: Expr, q: Anal, rollups: Map[RollupName, Anal]): Map[RollupName, Option[Expr]] = {
    rollups.mapValues { case r =>
      val rollupColIdx = r.selection.values.zipWithIndex.toMap
      rewriter.rewriteExpr(e, r, rollupColIdx)
    }
  }
}
