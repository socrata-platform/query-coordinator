package com.socrata.querycoordinator

import com.socrata.querycoordinator.QueryRewriter.{Anal, RollupName}
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery}

trait CompoundQueryRewriter { this: QueryRewriter =>

  def possibleRewrites(q: BinaryTree[Anal], rollups: Map[RollupName, BinaryTree[Anal]], debug: Boolean): (BinaryTree[Anal], Seq[String]) = {
    if (!debug) {
      return (q, Seq.empty)
    }

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

  protected def isPrefix(q: BinaryTree[Anal], r: BinaryTree[Anal], ruName: RollupName, debug: Boolean): Option[BinaryTree[Anal]] = {
    isPrefix(q, r, ruName, rightMost(q), debug)
  }

  private def isPrefix(q: BinaryTree[Anal], r: BinaryTree[Anal], ruName: RollupName, qRightMost: Leaf[Anal], debug: Boolean): Option[BinaryTree[Anal]] = {
    q match {
      case c@Compound(_, ql, qr) =>
        if (isEq(q, r, qRightMost)) {
          val rewritten = possibleRewrites(q.outputSchema.leaf, Map(ruName -> r.outputSchema.leaf)).get(ruName)
          rewritten.map(rw => Leaf(rw))
        } else if (isEq(ql, r, qRightMost)) {
          val rewritten = possibleRewrites(q.outputSchema.leaf, Map(ruName -> r.outputSchema.leaf)).get(ruName)
          rewritten.map(rw => PipeQuery(Leaf(rw), qr))
        } else {
          isPrefix(ql, r, ruName, qRightMost, debug) match {
            case Some(result@Compound(_, _, _)) =>
              Some(Compound(c.op, result, qr))
            case result =>
              result
          }
        }
      case _ =>
        None
    }
  }

  protected def isEq(q: BinaryTree[Anal], r: BinaryTree[Anal], qRightMost: Leaf[Anal]): Boolean = {
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

  protected def rightMost[T](bt: BinaryTree[T]): Leaf[T] = {
    bt match {
      case Compound(_, _, r) => rightMost(r)
      case l@Leaf(_) => l
    }
  }
}
