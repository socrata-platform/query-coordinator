package com.socrata.querycoordinator

import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.typed
import com.socrata.soql.types.{SoQLFloatingTimestamp, SoQLType}

class TestQueryRewriterDateTruncLtGte extends TestQueryRewriterDateTruncBase
  with TestQueryRewriterDateTruncLtGteNegative {
  val rewrittenQueryRymd = "SELECT c2 as ward, coalesce(sum(c3), 0) as count WHERE c1 >= '2011-03-08' AND c1 < '2019-08-02' GROUP BY c2"
  val rewrittenQueryAnalysisRymd = analyzeRewrittenQuery("r_ymd", rewrittenQueryRymd)

  val rewrittenQueryRym = "SELECT c2 as ward, coalesce(sum(c3), 0) as count WHERE c1 >= '2011-03-01' AND c1 < '2019-08-01' GROUP BY c2"
  val rewrittenQueryAnalysisRym = analyzeRewrittenQuery("r_ymd", rewrittenQueryRym)

  val rewrittenQueryRy = "SELECT c2 as ward, coalesce(sum(c3), 0) as count WHERE c1 >= '2011-01-01' AND c1 < '2019-01-01' GROUP BY c2"
  val rewrittenQueryAnalysisRy = analyzeRewrittenQuery("r_y", rewrittenQueryRy)

  test("year") {
    val q = "SELECT ward, count(*) AS count WHERE crime_date >= '2011-01-01' AND crime_date < '2019-01-01' GROUP BY ward"
    val rewrites = rewritesFor(q)

    rewrites should contain key "r_ymd"
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisRy)
    rewrites should contain key "r_ym"
    rewrites.get("r_ym").get should equal(rewrittenQueryAnalysisRy)
    rewrites should contain key "r_y"
    rewrites.get("r_y").get should equal(rewrittenQueryAnalysisRy)

    rewrites should have size 3
  }

  test("month") {
    val q = "SELECT ward, count(*) AS count WHERE crime_date >= '2011-03-01' AND crime_date < '2019-08-01' GROUP BY ward"
    val rewrites = rewritesFor(q)

    rewrites should contain key "r_ymd"
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisRym)
    rewrites should contain key "r_ym"
    rewrites.get("r_ym").get should equal(rewrittenQueryAnalysisRym)

    rewrites should have size 2
  }

  test("day") {
    val q = "SELECT ward, count(*) AS count WHERE crime_date >= '2011-03-08' AND crime_date < '2019-08-02' GROUP BY ward"
    val rewrites = rewritesFor(q)

    rewrites should contain key "r_ymd"
    rewrites.get("r_ymd").get should equal(rewrittenQueryAnalysisRymd)

    rewrites should have size 1
  }

  // Ensure that queries explicitly filtering out dates that javascript can't handle can hit rollups.
  test("9999 filter") {
    val q = "SELECT ward, count(*) AS count WHERE crime_date >= '2011-03-08' AND crime_date < '2019-08-02' AND crime_date < '9999-01-01' GROUP BY ward"
    val rewrites = rewritesFor(q)

    rewrites should contain key "r_ymd"
    rewrites.get("r_ymd").get should equal(analyzeRewrittenQuery("r_ymd",
      "SELECT c2 as ward, coalesce(sum(c3), 0) as count WHERE c1 >= '2011-03-08' AND c1 < '2019-08-02' AND c1 < '9999-01-01' GROUP BY c2"))
    rewrites should have size 1
  }


  // Ensure that queries explicitly filtering out nulls can hit rollups
  test("IS NOT NULL filter") {
    val q = "SELECT ward, count(*) AS count WHERE crime_date >= '2011-03-08' AND crime_date < '2019-08-02' AND crime_date IS NOT NULL GROUP BY ward"
    val rewrites = rewritesFor(q)

    rewrites should contain key "r_ymd"
    rewrites.get("r_ymd").get should equal(analyzeRewrittenQuery("r_ymd",
      "SELECT c2 as ward, coalesce(sum(c3), 0) as count WHERE c1 >= '2011-03-08' AND c1 < '2019-08-02' AND c1 IS NOT NULL GROUP BY c2"))
    rewrites should have size 1
  }

  test("shouldn't rewrite") {
    rewritesFor("SELECT ward WHERE crime_date < '2012-01-01T00:00:01'") should have size 0
    rewritesFor("SELECT ward WHERE crime_date <= '2012-01-01T00:00:00'") should have size 0
    rewritesFor("SELECT ward WHERE crime_date > '2012-01-01T00:00:00'") should have size 0
    rewritesFor("SELECT ward WHERE crime_date > '2012-01-01T01:00:00'") should have size 0
    rewritesFor("SELECT ward WHERE crime_date >= '2012-01-01T01:00:00' AND crime_date < '2013-01-01'") should have size 0
  }

  test("rewrite sum / count with implicit group by in query") {
    val q = "SELECT sum(number1)/count(number1) as avg WHERE crime_date >= '2011-01-01' AND crime_date < '2019-01-01'"
    val rq = "SELECT sum(c2) /  coalesce(sum(c3), 0) as avg WHERE c1 >= '2011-01-01' AND c1 < '2019-01-01'"
    val ra = analyzeRewrittenQuery("r_sca_ymd", rq)
    val rewrites = rewritesFor(q)
    rewrites should have size 1
    rewrites should contain key "r_sca_ymd"
    val rewrite = rewrites("r_sca_ymd")
    rewrite should equal(ra)
  }

  test("supports avg rewrite") {
    val q = "SELECT avg(number1) WHERE crime_date >= '2011-01-01' AND crime_date < '2019-01-01'"
    val rq = "SELECT sum(c2) /  sum(c3) as avg_number1 WHERE c1 >= '2011-01-01' AND c1 < '2019-01-01'"
    val ra = analyzeRewrittenQuery("r_sca_ymd", rq)
    val rewrites = rewritesFor(q)
    rewrites should have size 1
    rewrites should contain key "r_sca_ymd"
    val rewrite = rewrites("r_sca_ymd")
    rewrite should equal(ra)
  }
}
