package com.socrata.querycoordinator.caching.cache

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.soql.typed.Indistinct
import com.socrata.soql.{BinaryTree, SoQLAnalysis}
import com.socrata.soql.types.SoQLType
import org.joda.time.DateTime
import org.slf4j.Logger

trait CacheSessionProvider {
  protected val log: Logger

  def open(rs: ResourceScope, dataset: String): CacheSession

  val minQueryTimeMs: Long = -1

  private var disableTil = new DateTime(0)

  def disable() = {
    disableTil = new DateTime(System.currentTimeMillis()).plusHours(1)
    log.warn(s"disable cache until $disableTil" )
  }

  def disabled: Boolean = {
    disableTil.isAfterNow
  }

  def shouldSkip(analyses: BinaryTree[SoQLAnalysis[String, SoQLType]], rollupName: Option[String]): Boolean = {
    val last = analyses.outputSchema.leaf
    last.limit.isEmpty ||
    rollupName.isDefined ||
      !analyses.seq.exists { a =>
        a.where.isDefined ||
        a.groupBys.nonEmpty ||
        a.having.isDefined ||
        (a.orderBys.size > 1) ||
        a.search.isDefined ||
        !a.distinct.isInstanceOf[Indistinct[_, _]]
      }
  }

  def shouldSkip(queryTimeMs: Long): Boolean = {
    queryTimeMs < minQueryTimeMs
  }
}
