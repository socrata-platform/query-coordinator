package com.socrata.querycoordinator

import com.socrata.querycoordinator.rollups.{QueryRewriterImplementation}
import com.socrata.querycoordinator.rollups.QueryRewriter.{Anal, RollupName}
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.types.{SoQLType, SoQLValue}

class QueryRewriterWithJoinEnabled(analyzer: SoQLAnalyzer[SoQLType, SoQLValue]) extends QueryRewriterImplementation(analyzer) {
  override def possibleRewrites(q: Anal, rollups: Map[RollupName, Anal]): Map[RollupName, Anal] = {
    possibleRewrites(q, rollups, true)
  }
}
