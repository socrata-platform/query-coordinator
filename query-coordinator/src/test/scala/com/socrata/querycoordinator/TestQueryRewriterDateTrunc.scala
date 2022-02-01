package com.socrata.querycoordinator

class TestQueryRewriterDateTrunc extends TestQueryRewriterDateTruncBase {
  val rewrittenQueryRymd = "SELECT c2 as ward, coalesce(sum(c3), 0) as count WHERE c1 BETWEEN date_trunc_ymd('2011-02-01') " +
    "AND date_trunc_ymd('2012-05-02') GROUP BY c2"
  def rewrittenQueryAnalysisRymd = analyzeRewrittenQuery("r_ymd", rewrittenQueryRymd)

  val rewrittenQueryRym = "SELECT c2 as ward, coalesce(sum(c3), 0) as count WHERE c1 BETWEEN date_trunc_ym('2011-02-01') " +
    "AND date_trunc_ym('2012-05-02') GROUP BY c2"
  def rewrittenQueryAnalysisRym = analyzeRewrittenQuery("r_ymd", rewrittenQueryRym)

  val rewrittenQueryNotBetweenRym = "SELECT c2 as ward, coalesce(sum(c3), 0) as count WHERE c1 NOT BETWEEN " +
    "date_trunc_ym('2011-02-01') AND date_trunc_ym('2012-05-02') GROUP BY c2"
  def rewrittenQueryAnalysisNotBetweenRym = analyzeRewrittenQuery("r_ymd", rewrittenQueryNotBetweenRym)

  val rewrittenQueryRy = "SELECT c2 as ward, coalesce(sum(c3), 0) as count WHERE c1 BETWEEN date_trunc_y('2011-02-01') " +
    "AND date_trunc_y('2012-05-02') GROUP BY c2"
  def rewrittenQueryAnalysisRy = analyzeRewrittenQuery("r_y", rewrittenQueryRy)


  val qDateTruncOnLiterals = "SELECT ward, count(*) as count WHERE crime_date BETWEEN '2004-01-01' AND '2014-01-01' GROUP BY ward"

  test("Shouldn't rewrite date_trunc on literals") {
    val queryAnalysis = analyzeQuery(qDateTruncOnLiterals)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)
    rewrites should have size 0
  }

  val qDifferingAggregations = """
    SELECT ward, count(*) as count
     WHERE crime_date BETWEEN date_trunc_y('2004-01-01') AND date_trunc_ym('2014-01-01') GROUP BY ward"""

  test("Shouldn't rewrite date_trunc on differing aggregations") {
    val queryAnalysis = analyzeQuery(qDifferingAggregations)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)
    rewrites should have size 0
  }

  val qDateTruncCannotBeMapped = """
    SELECT ward, count(*) as count
     WHERE crime_date BETWEEN date_trunc_y(some_date) AND date_trunc_y('2014-01-01') GROUP BY ward"""

  test("Shouldn't rewrite date_trunc on expression that can't be mapped") {

    val queryAnalysis = analyzeQuery(qDateTruncCannotBeMapped)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)
    rewrites should have size 0
  }

  val qBetweenDateTruncYmd = """
    SELECT ward, count(*) AS count
     WHERE crime_date BETWEEN date_trunc_ymd('2011-02-01') AND date_trunc_ymd('2012-05-02')
     GROUP BY ward"""

  test("BETWEEN date_trunc_ymd") {
    val queryAnalysis = analyzeQuery(qBetweenDateTruncYmd)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key "r_ymd"
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisRymd)

    rewrites should have size 1
  }

  val qBetweenDateTruncYm = """
     SELECT ward, count(*) AS count
      WHERE crime_date BETWEEN date_trunc_ym('2011-02-01') AND date_trunc_ym('2012-05-02')
      GROUP BY ward"""

  test("BETWEEN date_trunc_ym") {
    val queryAnalysis = analyzeQuery(qBetweenDateTruncYm)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key "r_ymd"
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisRym)
    rewrites should contain key "r_ym"
    rewrites.get("r_ym").get should equal(rewrittenQueryAnalysisRym)

    rewrites should have size 2
  }

  val qBetweenDateTruncY = """
    SELECT ward, count(*) AS count
     WHERE crime_date BETWEEN date_trunc_y('2011-02-01') AND date_trunc_y('2012-05-02') GROUP BY ward"""

  test("BETWEEN date_trunc_y") {

    val queryAnalysis = analyzeQuery(qBetweenDateTruncY)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key "r_ymd"
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisRy)
    rewrites should contain key "r_ym"
    rewrites.get("r_ym").get should equal(rewrittenQueryAnalysisRy)
    rewrites should contain key "r_y"
    rewrites.get("r_y").get should equal(rewrittenQueryAnalysisRy)

    rewrites should have size 3
  }

  val qNotBetweenDateTruncYm = """
    SELECT ward, count(*) AS count
     WHERE crime_date NOT BETWEEN date_trunc_ym('2011-02-01') AND date_trunc_ym('2012-05-02')
     GROUP BY ward"""

  test("NOT BETWEEN date_trunc_ym") {

    val queryAnalysis = analyzeQuery(qNotBetweenDateTruncYm)

    val rewrites = rewriter.possibleRewrites(queryAnalysis, rollupAnalysis)

    rewrites should contain key "r_ymd"
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisNotBetweenRym)
    rewrites should contain key "r_ym"
    rewrites.get("r_ym").get should equal(rewrittenQueryAnalysisNotBetweenRym)

    rewrites should have size 2
  }
}
