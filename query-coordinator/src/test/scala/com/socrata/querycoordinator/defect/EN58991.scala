package com.socrata.querycoordinator.defect

import com.socrata.querycoordinator.util.QueryRewritingTestUtility.AssertRewriteDefault
import com.socrata.soql.types.{SoQLBoolean, SoQLFloatingTimestamp, SoQLNumber, SoQLText}
import org.scalatest.FunSuite

//https://socrata.atlassian.net/browse/EN-58991
//
//  Rollup has "avg" column - only matching if all other conditions are matching
//  ToDo: This edge case is not implemeneted yet
//
class EN58991 extends FunSuite{

  ignore("example one, should rewrite exact") {
    AssertRewriteDefault(
      Map(
        "_" -> Map(
          "stationname_column" -> (SoQLText.t, "stationname"),
          "stationlocation_column" -> (SoQLText.t, "stationlocation"),
          "datetime_column" -> (SoQLFloatingTimestamp.t, "datetime"),
          "recordid_column" -> (SoQLNumber.t, "recordid"),
          "roadsurfacetemperature_column" -> (SoQLNumber.t, "roadsurfacetemperature"),
          "airtemperature_column" -> (SoQLNumber.t, "airtemperature"),


        )
      ),
      Map(
        "one" -> "SELECT sum(`roadsurfacetemperature`) / count(`roadsurfacetemperature`) GROUP BY `stationname`"
      ),
      "SELECT sum(`roadsurfacetemperature`) / count(`roadsurfacetemperature`) GROUP BY `stationname`",
      "select c1",
      Some("one")
    )
  }
}
