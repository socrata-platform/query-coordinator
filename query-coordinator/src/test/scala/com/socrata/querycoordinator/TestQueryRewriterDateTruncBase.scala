package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryRewriter.{Anal, RollupName}
import com.socrata.querycoordinator.util.Join
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType

class TestQueryRewriterDateTruncBase extends TestQueryRewriterBase {

  import Join._

  /** Each rollup here is defined by:
    * - a name
    * - a soql statement.  Note this must be the mapped statement,
    *   i.e. non-system columns prefixed by an _, and backtick escaped
    * - a Seq of the soql types for each column in the rollup selection
    */
  val rollups = Seq(
    ("r_ymd", "SELECT date_trunc_ymd(`_crim-date`), `:wido-ward`, count(*) GROUP BY date_trunc_ymd(`_crim-date`), `:wido-ward`"),
    ("r_ym", "SELECT date_trunc_ym(`_crim-date`), `:wido-ward`, count(*) GROUP BY date_trunc_ym(`_crim-date`), `:wido-ward`"),
    ("r_y", "SELECT date_trunc_y(`_crim-date`), `:wido-ward`, count(*) GROUP BY date_trunc_y(`_crim-date`), `:wido-ward`"),
    ("r_sca_ymd", "SELECT date_trunc_ymd(`_crim-date`), sum(`_dxyz-num1`), count(`_dxyz-num1`), avg(`_dxyz-num1`) GROUP BY date_trunc_ymd(`_crim-date`)"),
    ("r_arithmetic_outside_aggregate_ymd", "SELECT date_trunc_ymd(`_crim-date`), sum(`_dxyz-num1`)*60, count(`_dxyz-num1`), avg(`_dxyz-num1`), `:wido-ward` GROUP BY date_trunc_ymd(`_crim-date`),`:wido-ward`")
  )

  val rollupInfos = rollups.map { x => new RollupInfo(x._1, x._2) }

  /** Pull in the rollupAnalysis for easier debugging */
  val rollupAnalysis = QueryRewriter.simpleRollups(rewriter.analyzeRollups(schema, rollupInfos, getSchemaWithFieldName))

  val rollupRawSchemas = rollupAnalysis.mapValues { case analysis: Anal =>
    analysis.selection.values.toSeq.zipWithIndex.map { case (expr, idx) =>
      rewriter.rollupColumnId(idx) -> expr.typ
    }.toMap
  }

  /** Analyze a "fake" query that has the rollup table column names in, so we
    * can use it to compare  with the rewritten one in assertions.
    */
  def analyzeRewrittenQuery(rollupName: String, q: String): SoQLAnalysis[String, SoQLType] = {
    val rewrittenRawSchema = rollupRawSchemas(rollupName)

    val rollupNoopColumnNameMap = rewrittenRawSchema.map { case (k, v) => ColumnName(k) -> k }

    val rollupDsContext = QueryParser.dsContext(rollupNoopColumnNameMap, rewrittenRawSchema)

    val rewrittenQueryAnalysis = analyzer.analyzeUnchainedQuery(q)(toAnalysisContext(rollupDsContext)).mapColumnIds(mapIgnoringQualifier(rollupNoopColumnNameMap))
    rewrittenQueryAnalysis
  }

  def rewritesFor(q: String): Map[RollupName, Anal] =
    rewriter.possibleRewrites(analyzeQuery(q), rollupAnalysis)
}
