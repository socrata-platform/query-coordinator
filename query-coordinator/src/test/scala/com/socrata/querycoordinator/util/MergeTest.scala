package com.socrata.querycoordinator.util

import com.socrata.querycoordinator.util.QueryRewritingTestUtility.AssertMergeDefault
import com.socrata.soql.types.SoQLNumber
import org.scalatest.FunSuite
import org.scalatest.Matchers.{an, be}
import org.scalatest.exceptions.TestFailedException

class MergeTest extends FunSuite {
  test("select * |> select * -> select *") {
    AssertMergeDefault(
      Map(
        "_" -> Map(
          "rowkey_column" -> (SoQLNumber.t, "rowkey"),
        )
      ),
      "select * |> select *",
      "select *"
    )
  }

  test("select *,1,2 |> select * -> select *,1,2") {
    AssertMergeDefault(
      Map(
        "_" -> Map(
          "rowkey_column" -> (SoQLNumber.t, "rowkey"),
        )
      ),
      "select *,1,2 |> select *",
      "select *,1,2"
    )
  }

  test("select *,1 |> select *,2 |> select * -> select *,1,2") {
    AssertMergeDefault(
      Map(
        "_" -> Map(
          "rowkey_column" -> (SoQLNumber.t, "rowkey"),
        )
      ),
      "select *,1 |> select *,2 |> select *",
      "select *,1,2"
    )
  }

  test("this one does merge, and this test should fail") {
    an [TestFailedException] should be thrownBy{
      AssertMergeDefault(
        Map(
          "_" -> Map(
            "rowkey_column" -> (SoQLNumber.t, "rowkey"),
          )
        ),
        "select *,1 |> select *,2 |> select *",
        //correct expected is 'select *,1,2'
        "select *,1 |> select *,2 |> select *"
      )
    }
  }

  test("this one does not merge") {
    AssertMergeDefault(
      Map(
        "_" -> Map(
          "rowkey_column" -> (SoQLNumber.t, "rowkey"),
        )
      ),
      "select * group by rowkey |> select count(*) as theCount",
      "select * group by rowkey |> select count(*) as theCount"
    )
  }
}
