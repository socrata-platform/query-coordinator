package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryRewriter.{Anal, RollupName}
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.types.{SoQLType, SoQLValue}

class QueryRewriterWithJoinEnabled(analyzer: SoQLAnalyzer[SoQLType, SoQLValue]) extends QueryRewriter(analyzer) {
  override def possibleRewrites(q: Anal, rollups: Map[RollupName, Anal]): Map[RollupName, Anal] = {
    possibleRewrites(q, rollups, true)
  }
}