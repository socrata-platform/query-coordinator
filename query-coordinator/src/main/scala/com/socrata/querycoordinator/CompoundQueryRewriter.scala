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
        isPrefix(q, ruAnalyses, ruName, debug) match {
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

  private def rewriteExact(q: Anal): Anal = {
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
      limit = None,
      offset = None,
      search = None,
      hints = Nil)
  }

  private def isPrefix(q: BinaryTree[Anal], r: BinaryTree[Anal], ruName: RollupName, debug: Boolean): Option[BinaryTree[Anal]] = {
    isPrefix(q, r, ruName, rightMost(q), debug)
  }

  private def isPrefix(q: BinaryTree[Anal], r: BinaryTree[Anal], ruName: RollupName, qRightMost: Leaf[Anal], debug: Boolean): Option[BinaryTree[Anal]] = {
    q match {
      case c@Compound(_, ql, qr) =>
        if (isEq(q, r, qRightMost)) {
          val rewritten = rewriteExact(q.outputSchema.leaf)
          Some(Leaf(rewritten))
        } else if (isEq(ql, r, qRightMost)) {
          val rewritten = rewriteExact(ql.outputSchema.leaf)
          Some(PipeQuery(Leaf(rewritten), qr))
        } else {
          isPrefix(ql, r, ruName, qRightMost, debug) match {
            case Some(result@Compound(_, _, _)) =>
              Some(Compound(c.op, result, qr))
            case result =>
              result
          }
        }
      case Leaf(_) if isEq(q, r, qRightMost)=>
        val rewritten = rewriteExact(q.outputSchema.leaf)
        Some(Leaf(rewritten))
      case _ =>
        None
    }
  }

  private def isEq(q: BinaryTree[Anal], r: BinaryTree[Anal], qRightMost: Leaf[Anal]): Boolean = {
    (q, r) match {
      case (Compound(_, ql, qr), Compound(_, rl, rr)) =>
        isEq(ql, rl, qRightMost) && isEq(qr, rr, qRightMost)
      case (Leaf(qf), Leaf(rf)) =>
        if (q.eq(qRightMost)) qf.copy(limit = None) == rf
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
