package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryRewriter.{Anal, ColumnRef, Expr, RollupName}
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery, SoQLAnalysis, typed}

/**
  * Rewrite compound query with either exact tree match or prefix tree match
  * TODO: Make matching more flexible.  e.g. "SELECT a, b" does not match "SELECT b, a" or "SELECT a"
  */
trait CompoundQueryRewriter { this: QueryRewriter =>

  /**
    * match if the whole tree is equal or prefix is equal
    */
  def possibleRewrites(q: BinaryTree[Anal], rollups: Map[RollupName, BinaryTree[Anal]], debug: Boolean): (BinaryTree[Anal], Seq[String]) = {
    if (!debug) {
      return (q, Seq.empty)
    }

    log.info("try rewrite compound query")
    val rewritten = rollups.foldLeft(Map.empty[RollupName, Option[BinaryTree[Anal]]]) { (acc, ru) =>
      if (acc.isEmpty) {
        val (ruName, ruAnalyses) = ru
        rewriteIfPrefix(q, ruAnalyses, ruName) match {
          case rewritten@Some(_) =>
            acc + (ruName -> rewritten)
          case _ =>
            acc
        }
      } else {
        acc
      }
    }

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
      distinct= false,
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

  private def rewriteIfPrefix(q: BinaryTree[Anal], r: BinaryTree[Anal], ruName: RollupName): Option[BinaryTree[Anal]] = {
    rewriteIfPrefix(q, r, ruName, rightMost(q))
  }

  private def rewriteIfPrefix(q: BinaryTree[Anal], r: BinaryTree[Anal], ruName: RollupName, qRightMost: Leaf[Anal]): Option[BinaryTree[Anal]] = {
    q match {
      case c@Compound(_, ql, qr) =>
        if (isEqual(q, r, qRightMost)) {
          val rewritten = rewriteExact(q.outputSchema.leaf, qRightMost.leaf)
          Some(Leaf(rewritten))
        } else if (isEqual(ql, r, qRightMost)) {
          val rewritten = rewriteExact(ql.outputSchema.leaf, qRightMost.leaf)
          Some(PipeQuery(Leaf(rewritten), qr))
        } else {
          rewriteIfPrefix(ql, r, ruName, qRightMost) match {
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
}
