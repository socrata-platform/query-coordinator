package com.socrata.querycoordinator.util

import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery, SoQLAnalysis}
import com.socrata.soql.types.SoQLType

object BinaryTreeHelper {

  /**
   * This uses object reference for comparison.
   */
  def replace[T <: AnyRef](analyses: BinaryTree[T], a: T, b: T): BinaryTree[T] = {
    analyses match {
      case c@Compound(op, l, r) =>
        val nl = replace(l, a, b)
        val nr = replace(r, a, b)
        Compound(op, left = nl, right = nr, c.inParen)
      case Leaf(analysis, inParen) =>
        if (analysis.eq(a)) Leaf(b, inParen) // object reference comparison
        else analyses
    }
  }

  def outerMostAnalyses[T](analyses: BinaryTree[SoQLAnalysis[T, SoQLType]], seq: Seq[SoQLAnalysis[T, SoQLType]] = Seq.empty): Seq[SoQLAnalysis[T, SoQLType]] = {
    analyses match {
      case PipeQuery(_, r, _) =>
        outerMostAnalyses(r, seq)
      case Compound(_, l, r) =>
        outerMostAnalyses(l, seq) ++ outerMostAnalyses(r, seq)
      case Leaf(analysis, _) =>
        seq :+ analysis
    }
  }
}
