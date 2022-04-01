package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryRewriter.{Anal, RollupName}
import com.socrata.querycoordinator.util.Join.toAnalysisContext
import com.socrata.querycoordinator.util.Join.mapIgnoringQualifier
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType

class TestQueryRewriterWindowFunction extends TestQueryRewriterBase {
  /** Each rollup here is defined by:
    * - a name
    * - a soql statement.  Note this must be the mapped statement,
    * i.e. non-system columns prefixed by an _, and backtick escaped
    * - a Seq of the soql types for each column in the rollup selection
    */
  val rollups: Seq[(String, String)] = Seq(
    ("r1", "SELECT `_crim-typ3`, `:wido-ward`, avg(`_dxyz-num1`) over (partition by `_crim-typ3`)"),
  )

  val rollupInfos = rollups.map { x => new RollupInfo(x._1, x._2) }

  /** Pull in the rollupAnalysis for easier debugging */
  val rollupAnalysis: Map[RollupName, Anal] = rewriter.analyzeRollups(schema, rollupInfos, Map.empty, getSchemaWithFieldName)

  val rollupRawSchemas = rollupAnalysis.mapValues { analysis: Anal =>
    analysis.selection.values.toSeq.zipWithIndex.map { case (expr, idx) =>
      rewriter.rollupColumnId(idx) -> expr.typ
    }.toMap
  }

  /** Analyze a "fake" query that has the rollup table column names in, so we
    * can use it to compare  with the rewritten one in assertions.
    */
  def analyzeRewrittenQuery(rollupName: String, q: String): SoQLAnalysis[String, SoQLType] = {
    val rewrittenRawSchema = rollupRawSchemas(rollupName)

    val rollupNoopColumnNameMap = rewrittenRawSchema.map { case (k, _) => ColumnName(k) -> k }

    val rollupDsContext = QueryParser.dsContext(rollupNoopColumnNameMap, rewrittenRawSchema)

    val rewrittenQueryAnalysis = analyzer.analyzeUnchainedQuery(q)(toAnalysisContext(rollupDsContext)).mapColumnIds(mapIgnoringQualifier(rollupNoopColumnNameMap))
    rewrittenQueryAnalysis
  }

  test("do not map window functions with different where") {
    val q = "SELECT crime_type, ward, avg(number1) over (partition by crime_type) WHERE ward = 23"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should have size 0
  }

  // We logically should be able to rewrite this and several other cases, but for simplicity the current code takes
  // a blanket "do not rewrite if there is a window function" approach.  This test is documentation of what we
  // _could_ enhance the code to rewrite.
  test("doesn't map window functions with no where and different selection (logically could succeed, but not implemented)") {
    val q = "SELECT crime_type, avg(number1) over (partition by crime_type)"
    val queryAnalysis = analyzeQuery(q)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should have size 0
  }
}
