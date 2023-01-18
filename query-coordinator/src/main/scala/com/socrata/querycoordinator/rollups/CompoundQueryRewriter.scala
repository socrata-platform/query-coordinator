package com.socrata.querycoordinator.rollups

import com.socrata.querycoordinator.rollups.QueryRewriter.{Anal, ColumnId, RollupName}
import com.socrata.soql.typed.{ColumnRef, CompoundRollup, Indistinct}
import com.socrata.soql.types.SoQLType
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery}

/**
  * Rewrite compound query with either exact tree match or prefix tree match
  * TODO: Make matching more flexible.  e.g. "SELECT a, b" does not match "SELECT b, a" or "SELECT a"
  */
trait CompoundQueryRewriter { this: QueryRewriterImplementation =>

  /**
    * The tree q is successfully rewritten and returned in the first tuple element if
    * the second tuple element is not empty which can either be an exact tree match or a prefix tree match.
    * Otherwise, the original q is returned.
    */
  def possibleRewrites(q: BinaryTree[Anal], rollups: Map[RollupName, BinaryTree[Anal]], requireCompoundRollupHint: Boolean): (BinaryTree[Anal], Seq[String]) = {
    if (requireCompoundRollupHint && !compoundRollup(q.outputSchema.leaf)) {
      return (q, Seq.empty)
    }

    log.info("try rewrite compound query")
    // lazy view because only the first one rewritten one is taken
    // Might consider all and pick the one with the best score like simple rollup
    val rewritten = rollups.view.map {
      case (ruName, ruAnalyses) =>
        val rewritten = rewriteIfPrefix(q, ruAnalyses)
        (ruName, rewritten)
    }.filter(_._2.isDefined)

    rewritten.headOption match {
      case Some((ruName, Some(ruAnalyses))) =>
        (ruAnalyses, Seq(ruName))
      case _ =>
        (q, Seq.empty)
    }
  }

  private def rewriteExact(q: Anal, qRightMost: Anal): Anal = {
    val columnMap = q.selection.values.zipWithIndex.map {
      case(expr, idx) =>
        (expr, ColumnRef(qualifier = None, column = rollupColumnId(idx), expr.typ.t)(expr.position))
    }.toMap
    val mappedSelection = q.selection.mapValues { expr => columnMap(expr)}
    q.copy(
      isGrouped = false ,
      distinct = Indistinct[ColumnId, SoQLType],
      selection = mappedSelection,
      joins = Nil,
      where = None,
      groupBys = Nil,
      having = None,
      orderBys = Nil,
      limit = qRightMost.limit,
      offset = qRightMost.offset,
      search = None,
      hints = Nil)
  }

  private def rewriteIfPrefix(q: BinaryTree[Anal], r: BinaryTree[Anal]): Option[BinaryTree[Anal]] = {
    rewriteIfPrefix(q, r, rightMost(q))
  }

  private def rewriteIfPrefix(q: BinaryTree[Anal], r: BinaryTree[Anal], qRightMost: Leaf[Anal]): Option[BinaryTree[Anal]] = {
    q match {
      case c@Compound(_, ql, qr) =>
        if (isEqual(q, r, qRightMost)) {
          val rewritten = rewriteExact(q.outputSchema.leaf, qRightMost.leaf)
          Some(Leaf(rewritten))
        } else if (isEqual(ql, r, qRightMost)) {
          val rewritten = rewriteExact(ql.outputSchema.leaf, qRightMost.leaf)
          Some(PipeQuery(Leaf(rewritten), qr))
        } else {
          rewriteIfPrefix(ql, r, qRightMost) match {
            case Some(result@Compound(_, _, _)) =>
              Some(Compound(c.op, result, qr))
            case result =>
              result
          }
        }
      case Leaf(_) if isEqual(q, r, qRightMost)=>
        val rewritten = rewriteExact(q.outputSchema.leaf, qRightMost.leaf)
        Some(Leaf(rewritten))
      case _ =>
        None
    }
  }

  /**
    * q is equal r ignoring limit and offset of the right most of q
    */
  private def isEqual(q: BinaryTree[Anal], r: BinaryTree[Anal], qRightMost: Leaf[Anal]): Boolean = {
    (q, r) match {
      case (Compound(qo, ql, qr), Compound(ro, rl, rr)) if qo == ro =>
        isEqual(ql, rl, qRightMost) && isEqual(qr, rr, qRightMost)
      case (Leaf(qf), Leaf(rf)) =>
        if (q.eq(qRightMost)) qf.copy(limit = None, offset = None) == rf
        else qf == rf
      case _ =>
        false
    }
  }

  private def rightMost[T](bt: BinaryTree[T]): Leaf[T] = {
    bt match {
      case Compound(_, _, r) => rightMost(r)
      case l@Leaf(_) => l
    }
  }

  private def compoundRollup(q: Anal): Boolean = {
    q.hints.exists {
      case CompoundRollup(_) => true
      case _ => false
    }
  }

}
