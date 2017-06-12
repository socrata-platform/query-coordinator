package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryParser.{RowLimitExceeded, SuccessfulParse}
import com.socrata.querycoordinator.SchemaFetcher.SuccessfulExtendedSchema
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.types.{SoQLText, SoQLType}
import org.joda.time.DateTime

class RowsLimitTest extends TestBase {

  import RowsLimitTest._

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)


  test("max rows and default rows limit are respected") {
    val defaultRowLimit = 20
    val maxRowLimit = 200

    val qp = new QueryParser(analyzer, new FakeSchemaFetcher(extendedSchema), Some(maxRowLimit), defaultRowLimit)
    (qp.apply(datasetId, "select *", schema, fakeRequestBuilder) match {
      case SuccessfulParse(analyses) => analyses.last.limit
      case _ =>
    }) should be(Some(defaultRowLimit))

    (qp.apply(datasetId, s"select * limit $maxRowLimit", schema, fakeRequestBuilder) match {
      case SuccessfulParse(analyses) => analyses.last.limit
      case _ =>
    }) should be(Some(maxRowLimit))


    (qp.apply(datasetId, s"select * limit ${maxRowLimit + 1}", schema, fakeRequestBuilder) match {
      case x@RowLimitExceeded(_) => x
      case _ =>
    }) should be(RowLimitExceeded(maxRowLimit))

  }

  test("max rows not configured") {
    val defaultRowLimit = 20

    val qp = new QueryParser(analyzer, new FakeSchemaFetcher(extendedSchema), None, defaultRowLimit)
    (qp.apply(datasetId, "select *", schema, fakeRequestBuilder) match {
      case SuccessfulParse(analyses) => analyses.last.limit
      case _ =>
    }) should be(Some(defaultRowLimit))

    (qp.apply(datasetId, "select * limit 100000", schema, fakeRequestBuilder) match {
      case SuccessfulParse(analyses) => analyses.last.limit
      case _ =>
    }) should be(Some(100000))
  }
}

object RowsLimitTest {
  val truthSchema = Map[String, Tuple2[SoQLType, String]]("c" -> Tuple2(SoQLText, "c"))
  val schemaWithFieldName = SchemaWithFieldName(truthSchema.hashCode().toString, truthSchema, pk = ":id")
  val extendedSchema = SuccessfulExtendedSchema(schemaWithFieldName, 0, 0, new DateTime(0))
  val schema: Map[String, SoQLType] = truthSchema.map { case (k, v) => (k -> v._1) }
}