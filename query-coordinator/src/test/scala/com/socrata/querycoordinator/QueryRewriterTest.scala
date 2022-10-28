package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryRewritingTestUtility._
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.parsing.{AbstractParser, Parser}
import com.socrata.soql.types.{SoQLFloatingTimestamp, SoQLNumber, SoQLText}
import org.scalatest.FunSuite

class QueryRewriterTest extends FunSuite {

  // This is a sample test scenario for query rewriting
  // This lays out what I believe to be a self-contained and easier way of testing rewrites
  // There is no shared global state
  // There is a lot of functional stuff going on, but hopefully you wont need to care
  test("testPossibleRewritesRaw") {
    // Some objects that are used throughout, could probably be shared and put into a beforeall
    val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
    val parserParams = AbstractParser.Parameters(allowJoins = true)
    val parser = new Parser(parserParams)
    val rewriter = new QueryRewriter(analyzer)


    // Given dataset definitions (multiple datasets)
    // And rollup definitions (multiple rollups)
    // And a query
    // And the expected outcome (which looks like Seq(<analyzed soql>,Seq(<used rollup name>)))
    // Then assert the rewritten query matches the expected outcome
    //
    // This function is curried, and the first argument group it takes in are:
    //   a. A soql parser
    //   b. A soql analyzer - this is curried for easier building/reuse
    //   c. A column mapping function - again curried so we can dynamically pass the relevant columns and do the mapping later
    //   d. The rewrite function - which should call some sort of rewriter, with the important arguments being analyzedSoql and analyzedRollups
    // The second argument group consists of the raw data we actual care about acting on:
    //   a. Dataset definitions
    //   b. Rollup definitions
    //   c. Query definition
    //   d. Function that takes in a raw query and rollup name (should be apart of the above rollup definitions), and returns a crazy function that is used to build what the rewritten query should be
    AssertRewrite(parser.binaryTreeSelect, Analyze(analyzer), ReMap, (a, b) => rewriter.possibleRewrites(a, b, false))(
      // The dataset definitions
      Map("_" -> Map("dxyz-num1" -> (SoQLNumber.t, "number1"),
        ":wido-ward" -> (SoQLNumber.t, "ward"),
        "crim-typ3" -> (SoQLText.t, "crime_type"),
        "dont-roll" -> (SoQLText.t, "dont_create_rollups"),
        "crim-date" -> (SoQLFloatingTimestamp.t, "crime_date"),
        "some-date" -> (SoQLFloatingTimestamp.t, "some_date")
      ),
        "_tttt-tttt" -> Map("crim-typ3" -> (SoQLText.t, "crime_type"),
          "aaaa-aaaa" -> (SoQLText.t, "aa"),
          "bbbb-bbbb" -> (SoQLText.t, "bb"),
          "dddd-dddd" -> (SoQLFloatingTimestamp.t, "floating"),
          "nnnn-nnnn" -> (SoQLNumber.t, "nn"))
      ),
      // The rollup definitions
      Map(
        "one" -> "select count(ward)"
      ),
      // The query definition
      "select count(ward) as count_ward",
      // Function that mainly takes in the expected query and rollup name, but is curried and used to provide the needed context during runtime
      AnalyzeRewrittenFromRollup(
        // The expected rewritten query
        "select c1 as count_ward",
        // The rollup we expect to be used
        "one"
      )
    )
  }

  test("testPossibleRewritesDefault") {
    // Assumes a default setup, ie.. SoQLAnalyzer/AbstractParser.Parameters/Parser/QueryRewriter
    AssertRewriteDefault(
      Map("_" -> Map("dxyz-num1" -> (SoQLNumber.t, "number1"),
        ":wido-ward" -> (SoQLNumber.t, "ward"),
        "crim-typ3" -> (SoQLText.t, "crime_type"),
        "dont-roll" -> (SoQLText.t, "dont_create_rollups"),
        "crim-date" -> (SoQLFloatingTimestamp.t, "crime_date"),
        "some-date" -> (SoQLFloatingTimestamp.t, "some_date")
      ),
        "_tttt-tttt" -> Map("crim-typ3" -> (SoQLText.t, "crime_type"),
          "aaaa-aaaa" -> (SoQLText.t, "aa"),
          "bbbb-bbbb" -> (SoQLText.t, "bb"),
          "dddd-dddd" -> (SoQLFloatingTimestamp.t, "floating"),
          "nnnn-nnnn" -> (SoQLNumber.t, "nn"))
      ),
      Map(
        "one" -> "select count(ward)"
      ),
      "select count(ward) as count_ward",
      "select c1 as count_ward",
      "one"
    )
  }
}
