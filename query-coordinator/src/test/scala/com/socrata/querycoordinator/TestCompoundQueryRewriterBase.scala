package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryRewriter.{Anal, ColumnId, RollupName}
import com.socrata.soql.{BinaryTree, SoQLAnalysis, SoQLAnalyzer}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.parsing.{AbstractParser, Parser}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.{SoQLFloatingTimestamp, SoQLNumber, SoQLText, SoQLType}

trait TestCompoundQueryRewriterBase { this: TestBase =>

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  val rewriter = new QueryRewriterWithJoinEnabled(analyzer)
  val rollupAnalysis: Map[RollupName, Anal]
  val rollupAnalyses: Map[RollupName, BinaryTree[Anal]] = Map.empty
  val rollups: Iterable[_]

  val allSchemaWithFieldName: Map[String, Map[String, (SoQLType, String)]] =
    Map("_" -> Map("dxyz-num1" -> (SoQLNumber.t, "number1"),
                   ":wido-ward" -> (SoQLNumber.t, "ward"),
                   "crim-typ3" -> (SoQLText.t, "crime_type"),
                   "dont-roll" -> (SoQLText.t, "dont_create_rollups"),
                   "crim-date" -> (SoQLFloatingTimestamp.t, "crime_date"),
                   "some-date" -> (SoQLFloatingTimestamp.t, "some_date")
                  ),
        "_tttt-tttt" -> Map("crim-typ3" -> (SoQLText.t, "crime_type"),
                            "aaaa-aaaa" -> (SoQLText.t, "aa"),
                            "bbbb-bbbb" -> (SoQLText.t, "bb"),
                            "dddd-dddd" -> (SoQLFloatingTimestamp.t, "floating"),
                            "nnnn-nnnn" -> (SoQLNumber.t, "nn"))
      )

  val allDatasetContext: Map[String, DatasetContext[SoQLType]] = allSchemaWithFieldName.mapValues { schemaWithFieldName =>
    val columnNameToType = schemaWithFieldName.map {
      case (_iColumnId, (typ, fieldName)) =>
        (ColumnName(fieldName), typ)
    }
    new DatasetContext[SoQLType] {
      val schema: OrderedMap[ColumnName, SoQLType] =
        OrderedMap[ColumnName, SoQLType](columnNameToType.toSeq:  _*)
    }
  }

  //  This is the content of allDatasetContext: Map[String, DatasetContext[SoQLType]] = Map(
  //    "_" -> new DatasetContext[SoQLType] {
  //      val schema =
  //        OrderedMap[ColumnName, SoQLType](
  //        ColumnName("number1") -> SoQLNumber.t,
  //        ColumnName("ward") -> SoQLNumber.t,...
  //    },
  //    "_tttt-tttt" -> new DatasetContext[SoQLType] {
  //      val schema =
  //        OrderedMap[ColumnName, SoQLType](
  //          ColumnName("crime_type") -> SoQLText.t,
  //          ColumnName("aa") -> SoQLText.t,...
  //    }
  //  )

  val allQualifiedColumnNameToIColumnId: Map[QualifiedColumnName, String] = allSchemaWithFieldName.foldLeft(Map.empty[QualifiedColumnName, String]) { (acc, entry) =>
    val (tableName, schemaWithFieldName) = entry
    val map = schemaWithFieldName.map {
      case (iColumnId, (_typ, fieldName)) =>
        val qual = if (tableName == "_") None else Some(tableName)
        QualifiedColumnName(qual, ColumnName(fieldName)) -> iColumnId
    }
    acc ++ map
  }

  //  This is the content of allQualifiedColumnNameToIColumnId: Map[QualifiedColumnName, String] = {
  //    Map(
  //      QualifiedColumnName(None, ColumnName("number1")) -> "dxyz-num1",
  //      QualifiedColumnName(None, ColumnName("ward")) -> ":wido-ward",
  //      QualifiedColumnName(None, ColumnName("crime_type")) -> "crim-typ3",
  //      QualifiedColumnName(None, ColumnName("dont_create_rollups")) -> "dont-roll",
  //      QualifiedColumnName(None, ColumnName("crime_date")) -> "crim-date",
  //      QualifiedColumnName(None, ColumnName("some_date")) -> "some-date",
  //      QualifiedColumnName(Some("_tttt-tttt"), ColumnName("crime_type")) -> "crim-typ3",
  //      QualifiedColumnName(Some("_tttt-tttt"), ColumnName("aa")) -> "aaaa-aaaa",
  //      QualifiedColumnName(Some("_tttt-tttt"), ColumnName("bb")) -> "bbbb-bbbb",
  //      QualifiedColumnName(Some("_tttt-tttt"), ColumnName("floating")) -> "dddd-dddd",
  //      QualifiedColumnName(Some("_tttt-tttt"), ColumnName("nn")) -> "nnnn-nnnn"
  //    )
  //  }

  def analyzeQuery(q: String): SoQLAnalysis[ColumnId, SoQLType] = {
    analyzeCompoundQuery(q).outputSchema.leaf
  }

  def analyzeCompoundQuery(q: String): BinaryTree[SoQLAnalysis[ColumnId, SoQLType]] = {
    val parserParams = AbstractParser.Parameters(allowJoins = true)
    val parsed = new Parser(parserParams).binaryTreeSelect(q)
    val analyses = analyzer.analyzeBinary(parsed)(allDatasetContext)
    QueryParser.remapAnalyses(allQualifiedColumnNameToIColumnId, analyses)
  }

  def rollupSchema(ruQuery: String): Map[ColumnName, SoQLType] = {
    val ruAnalyses = analyzeCompoundQuery(ruQuery).outputSchema.leaf
    ruAnalyses.selection.values.zipWithIndex.map {
      case (expr: CoreExpr[_, _], idx) =>
        (ColumnName(s"c${idx + 1}"), expr.typ.t)
    }.toMap
  }

  def rollupContext(ruQuery: String): Map[String, DatasetContext[SoQLType]] = {
    val map = rollupSchema(ruQuery)
    Map("_" -> new DatasetContext[SoQLType] {
      val schema = OrderedMap[ColumnName, SoQLType](map.toSeq: _*)
    })
  }

  def rollupColumnIds(ruQuery: String): Map[QualifiedColumnName, String] = {
    rollupSchema(ruQuery).map {
      case (cn, _typ) =>
        QualifiedColumnName(None, cn) -> cn.name
    }
  }

  def analyzeRewrittenCompoundQuery(q: String, ruQuery: String): BinaryTree[SoQLAnalysis[ColumnId, SoQLType]] = {
    val parserParams =
      AbstractParser.Parameters(
        allowJoins = true
      )
    val parsed = new Parser(parserParams).binaryTreeSelect(q)
    val ruCtx = rollupContext(ruQuery)
    val ctx = allDatasetContext ++ ruCtx
    val result = analyzer.analyzeBinary(parsed)(ctx)
    val columnMap = allQualifiedColumnNameToIColumnId ++ rollupColumnIds(ruQuery)
    QueryParser.remapAnalyses(columnMap, result)
  }

  def assertNoRollupMatch(q: String): Unit = {
    val queryAnalysis = analyzeQuery(q)
    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)
    rewrites should have size 0
  }

  def checkQueryRewrite(query: String, rollups: Map[String, String], expectedRollupName: String, expectedRewrittenQuery: String): Unit = {
    val queryAnalysis = analyzeCompoundQuery(query)
    val (rewrittens, Seq(ruNameApplied)) = rewriter.possibleRewrites(queryAnalysis, rollupAnalyses, true)
    val rollupQuery = rollups(expectedRollupName)
    val rewrittenQueryAnalysis = analyzeRewrittenCompoundQuery(expectedRewrittenQuery, rollupQuery)
    ruNameApplied should equal(expectedRollupName)
    rewrittens should equal(rewrittenQueryAnalysis)
  }
}
