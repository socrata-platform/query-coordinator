package com.socrata.querycoordinator.defect

import com.socrata.querycoordinator.util.QueryRewritingTestUtility.AssertRewriteDefault
import com.socrata.soql.types.{SoQLBoolean, SoQLFloatingTimestamp, SoQLNumber, SoQLText}
import org.scalatest.FunSuite

class RoboTest extends FunSuite {
  test("robo slack example") {
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "timezone" -> (SoQLText.t, "timezone"),
        )
      ),
      Map(
        "one" -> "SELECT timezone, count(*) GROUP BY timezone"
      ),
      "SELECT timezone, count(*) GROUP BY timezone |> SELECT count(*) as ct",
      "select c1 as timezone, coalesce(c2, 0) as count  |> SELECT count(*) as ct ",
      Some("one")
    )
  }
}
