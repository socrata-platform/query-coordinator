package com.socrata.querycoordinator.rollups

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonUtil}
import com.socrata.querycoordinator.TestBase
import com.socrata.querycoordinator._
import com.socrata.soql._
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.types.{SoQLFloatingTimestamp, SoQLType}

class TestExpressionRewriterDates extends TestBase {

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  private val queryRewriter = new QueryRewriterWithJoinEnabled(analyzer)

  val rewriter = queryRewriter.rewriteExpr

  test("truncatedTo") {
    def ts(s: String): SoQLFloatingTimestamp =
      SoQLFloatingTimestamp.apply(SoQLFloatingTimestamp.StringRep.unapply(s).get)

    rewriter.truncatedTo(ts("2012-01-01")) should be(Some(FloatingTimeStampTruncY))
    rewriter.truncatedTo(ts("2012-05-01")) should be(Some(FloatingTimeStampTruncYm))
    rewriter.truncatedTo(ts("2012-05-09")) should be(Some(FloatingTimeStampTruncYmd))
    rewriter.truncatedTo(ts("2012-05-09T01:00:00")) should be(None)
    rewriter.truncatedTo(ts("2012-05-09T00:10:00")) should be(None)
    rewriter.truncatedTo(ts("2012-05-09T00:00:02")) should be(None)
    rewriter.truncatedTo(ts("2012-05-09T00:00:00.001")) should be(None)
  }

}
