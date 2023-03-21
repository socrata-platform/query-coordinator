package com.socrata.querycoordinator.defect

import com.socrata.querycoordinator.QueryRewritingTestUtility.AssertRewriteDefault
import com.socrata.soql.types.{SoQLBoolean, SoQLFloatingTimestamp, SoQLNumber, SoQLText}
import org.scalatest.FunSuite

class Cheetah extends FunSuite{
  test("chained query group by test") {
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "make_column" -> (SoQLText.t, "make"),
          "country_column" -> (SoQLText.t, "country"),
          "timezone_column" -> (SoQLNumber.t, "timezone"),
          "phone_column" -> (SoQLText.t, "phone")
        )
      ),
      Map(
        "one" -> "SELECT `timezone` AS `timezone`, `country` AS `country`, count(*) AS `count` GROUP BY `timezone`, `country`"
      ),
      "SELECT timezone, count(*) GROUP BY timezone |> SELECT count(*) as ct",
      "SELECT c1 as timezone,(coalesce((sum(c3)),0)) as count GROUP BY c1 |> SELECT count(*) as ct",
      Some("one")
    )
  }
}
