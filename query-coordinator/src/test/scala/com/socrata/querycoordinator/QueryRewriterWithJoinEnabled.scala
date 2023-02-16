package com.socrata.querycoordinator

import com.socrata.querycoordinator.rollups.{QueryRewriterImplementation}
import com.socrata.querycoordinator.rollups.QueryRewriter.{Analysis, RollupName}
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.types.{SoQLType, SoQLValue}

class QueryRewriterWithJoinEnabled(analyzer: SoQLAnalyzer[SoQLType, SoQLValue]) extends QueryRewriterImplementation(analyzer) {
  override def possibleRewrites(q: Analysis, rollups: Map[RollupName, Analysis]): Map[RollupName, Analysis] = {
    possibleRewrites(q, rollups, true)
  }
}
