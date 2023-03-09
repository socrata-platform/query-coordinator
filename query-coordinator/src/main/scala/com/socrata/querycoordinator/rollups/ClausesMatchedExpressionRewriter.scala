package com.socrata.querycoordinator.rollups

import com.socrata.querycoordinator.rollups.QueryRewriter.{Analysis, Expr}
import com.socrata.querycoordinator.rollups.QueryRewriterImplementation.{FunctionCall, Where}
import com.socrata.querycoordinator.util.Join.NoQualifier
import com.socrata.soql.typed

/*
This class handles a specific case of the ExpressionRewriter where we know that all clauses (where/groupby/having) match,
so therefor we want to rewrite the window functions.
This class mostly exists as to not have to pass around an extra parameter recursively throughout the original rewriter.
The decision of which class to use is made in com.socrata.querycoordinator.rollups.QueryRewriterImplementation.rewriteSelection
*/
class ClausesMatchedExpressionRewriter(override val rollupColumnId: (Int) => String,
                                       override val rewriteWhere: (Option[Expr], Analysis, Map[Expr, Int]) => Option[Where]) extends ExpressionRewriter(rollupColumnId, rewriteWhere) {
  /** Recursively maps the Expr based on the rollupColIdx map, returning either
    * a mapped expression or None if the expression couldn't be mapped.
    *
    * Note that every case here needs to ensure to map every expression recursively
    * to ensure it is either a literal or mapped to the rollup.
    */
  override def apply(e: Expr, r: Analysis, rollupColIdx: Map[Expr, Int]): Option[Expr] = {
    e match {
      //At this point our where/groupby/having clauses match.
      //We are trying to do an exact rewrite, but only if there is a window function while all clauses match.
      //Lets check if any functionCalls in the expression chain contains a window function
      case fc: FunctionCall if extractFunctionCallChain(fc).exists(_.window.nonEmpty) =>
        //and try to rewrite it at the coarsest level
        rollupColIdx.get(fc).map(cid => typed.ColumnRef(NoQualifier, rollupColumnId(cid), fc.typ)(fc.position))
          .orElse(super.apply(e, r, rollupColIdx))
      case _ =>
        super.apply(e, r, rollupColIdx)
    }
  }

  //Walks down the functionCall parameters and collects all functionCalls
  private def extractFunctionCallChain(functionCall: FunctionCall): Seq[FunctionCall]={
    functionCall.parameters.flatMap{
      case a:FunctionCall => extractFunctionCallChain(a)
      case _=>Seq.empty
    }:+functionCall
  }
}
