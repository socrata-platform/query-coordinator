package com.socrata.querycoordinator.caching.cache

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.soql.SoQLAnalysis
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

  def shouldSkip(analyses: Seq[SoQLAnalysis[String, SoQLType]], rollupName: Option[String]): Boolean = {
    analyses.last.limit.isEmpty ||
    rollupName.isDefined ||
      !analyses.exists { a =>
        a.where.isDefined ||
        a.groupBy.nonEmpty ||
        a.having.isDefined ||
        a.orderBy.size > 1 ||
        a.search.isDefined ||
        a.distinct
      }
  }

  def shouldSkip(queryTimeMs: Long): Boolean = {
    queryTimeMs < minQueryTimeMs
  }
}
