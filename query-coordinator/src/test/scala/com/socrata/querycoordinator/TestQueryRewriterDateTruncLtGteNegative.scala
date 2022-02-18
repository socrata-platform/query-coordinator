package com.socrata.querycoordinator

import com.socrata.soql.typed
import com.socrata.soql.types.SoQLType

trait TestQueryRewriterDateTruncLtGteNegative { this: TestQueryRewriterDateTruncLtGte =>
  test("demonstrate rewritten query is wrong when SELECT contains arithmetic outside aggregate function, not a regression") {
    val q = "SELECT sum(number1) * 60 as sumx60 WHERE crime_date >= '2011-01-01' AND crime_date < '2019-01-01' GROUP BY ward"
    val rewrites = rewritesFor(q)
    rewrites should have size 1
    rewrites should contain key "r_arithmetic_outside_aggregate_ymd"
    val rewrite = rewrites("r_arithmetic_outside_aggregate_ymd")
    rewrite.selection should have size 1
    rewrite.groupBys should have size 1
    assertThrows[Exception] {
      rewrite.selection.values.head match {
        case _: typed.ColumnRef[_, SoQLType] =>
          // wrong rewrite: SELECT c2 as sumx60 WHERE c1 >= '2011-01-01' AND c1 < '2019-01-01' GROUP BY c5
          // correct rewrite should be: SELECT sum(c2) as sumx60 WHERE c1 >= '2011-01-01' AND c1 < '2019-01-01' GROUP BY c5
          throw new Exception("SELECT field GROUP BY g1... is wrong")
        case _ =>
      }
    }
  }

  test("does not support avg rewrite") {
    val q = "SELECT avg(number1) WHERE crime_date >= '2011-01-01' AND crime_date < '2019-01-01'"
    val rewrites = rewritesFor(q)
    rewrites should have size 0
  }
}
