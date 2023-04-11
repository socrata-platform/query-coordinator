package com.socrata.querycoordinator.rollups

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.querycoordinator._
import com.socrata.querycoordinator.rollups.QueryRewriter.{AnalysisTree, RollupName}
import com.socrata.soql._
import com.socrata.soql.environment.{TableName}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}

/**
  *  How to use MergeAnalysisTest:
  *
  *
  *  1. Create new test case in resources/rollups/analysis_merge_test_configs
  *     Test case json file format:
  *     {
  *       "schemas": {
  *         "_": {                                   <- root schema
  *           "2dup-k82r": ["number", "rowkey"]      <- column fxf, column type and column name
  *         },
  *         "ep9t-zk9z": {
  *           "47rk-qiqd": ["text", "county"]
  *         }
  *       },
  *       "rollups": {                                <- rollup as stored in pg_secondary rollup_map
  *         "r1": "SELECT `_93hn-8t73` AS `casenumber`, max(`_u4tu-5655`) AS `submitted_` GROUP BY `casenumber`"
  *       },
  *       "tests": {                                  <- merges for the rollup definitions
  *         "r1": "SELECT "
  *       }
  *     }
  *  2. Get the column names, fxf and types from the pg_secondary rollup_map and column_map
  *       (filter column map by dataset id)
  *  3. Keep test case files once done debugging.
  */

class MergeAnalysisTest extends BaseConfigurableRollupTest {

  case class TestConfig(schemas: Map[String, SchemaConfig], pipes: Map[String, String], merges: Map[String, String])

  object TestConfig {
    implicit val jCodec = AutomaticJsonCodecBuilder[TestConfig]
  }

  //
  // System Under Test
  //
  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  val rewriter: BaseQueryRewriter = new CompoundQueryRewriter(analyzer)

  //
  //  Fetchers for QueryRewriter
  //
  def fetchRollupInfo(config: TestConfig): Seq[RollupInfo] = config.pipes.map({
    case (k, v) => RollupInfo(k, v)
  }).toList

  def getSchemaByTableName(tableName: TableName, config: TestConfig): SchemaWithFieldName = {
    SchemaWithFieldName(
      tableName.name,
      config.schemas.getOrElse(tableName.name, Map.empty),
      ":pk"
    )
  }

  //
  //  Generic method to load and run test
  //
  private def loadAndRunTests(configFile: String) {
    val config = getConfig[TestConfig](configFile)
    val schema = Schema(
      "",
      config.schemas.getOrElse("_", Map.empty).map({ case (k, v) => (k, v._1)}),
      ":pk"
    )

    val rollupFetcher = () => fetchRollupInfo(config)
    val schemaFetcher = (tableName) => getSchemaByTableName(tableName, config)

    val rollups: Map[RollupName, AnalysisTree] = rollupFetcher() match {
      case Seq() => Map.empty
      case rollups => rewriter.analyzeRollups(schema, rollups, schemaFetcher)
    }

    rollups.size should equal(config.merges.size)

    rollups.foreach({ case (name, analysis) =>
      val merged = QueryRewriter.mergeAnalysis(analysis)
      merged.toString should equal(config.merges(name))
    })

  }

  //
  //  Actual test
  //
  test("pipes merge tests") {
    loadAndRunTests("rollups/analysis_merge_test_configs/test_pipes_1.json")
    loadAndRunTests("rollups/analysis_merge_test_configs/test_pipes_2.json")
    loadAndRunTests("rollups/analysis_merge_test_configs/test_pipes_3.json")
  }

}
