package com.socrata.querycoordinator.rollups

import com.socrata.querycoordinator.rollups.QueryRewriter.{Analysis, Expr}
import com.socrata.querycoordinator.rollups.QueryRewriterImplementation.{FunctionCall, Where}
import com.socrata.querycoordinator.util.Join.NoQualifier
import com.socrata.soql.typed

class ClauseAwareExpressionRewriter(override val rollupColumnId: (Int) => String,
                                    override val rewriteWhere: (Option[Expr], Analysis, Map[Expr, Int]) => Option[Where]) extends ExpressionRewriter(rollupColumnId, rewriteWhere) {
  /** Recursively maps the Expr based on the rollupColIdx map, returning either
   * a mapped expression or None if the expression couldn't be mapped.
   *
   * Note that every case here needs to ensure to map every expression recursively
   * to ensure it is either a literal or mapped to the rollup.
   */
   override def apply(e: Expr, r: Analysis, rollupColIdx: Map[Expr, Int]): Option[Expr] = {
    e match{
      case fc: FunctionCall if fc.window.nonEmpty =>
        Some(typed.ColumnRef(NoQualifier, rollupColumnId(rollupColIdx(fc)), fc.typ)(fc.position))
      case _=>
        super.apply(e, r, rollupColIdx)
    }
  }
}
