package com.socrata.querycoordinator

import com.socrata.soql.typed
import com.socrata.soql.types.SoQLType

trait TestQueryRewriterDateTruncLtGteNegative { this: TestQueryRewriterDateTruncLtGte =>

  test("SELECT contains arithmetic outside aggregate function") {
    val q = "SELECT sum(number1) * 60 as sumx60 WHERE crime_date >= '2011-01-01' AND crime_date < '2019-01-01' GROUP BY ward"
    val rewrites = rewritesFor(q)
    rewrites should have size 0
  }

}
