package com.socrata.querycoordinator.util

import com.socrata.querycoordinator.util.QueryRewritingTestUtility.{AssertMergeDefault, AssertRewriteDefault}
import com.socrata.soql.types.{SoQLBoolean, SoQLFloatingTimestamp, SoQLNumber, SoQLText}
import org.scalatest.FunSuite

class MergeTest extends FunSuite{
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
}
