package com.socrata.querycoordinator.caching.cache

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types.SoQLType
import org.joda.time.DateTime
import org.slf4j.Logger

trait CacheSessionProvider {
  protected val log: Logger

  def open(rs: ResourceScope, dataset: String): CacheSession

  private var disabledTil = new DateTime(0)

  def disabled() = {
    disabledTil = new DateTime(System.currentTimeMillis()).plusHours(1)
    log.warn(s"disable cache until $disabledTil" )
  }

  def isDisabled(): Boolean = {
    disabledTil.isAfterNow
  }

  def shouldSkip(analyses: Seq[SoQLAnalysis[String, SoQLType]], rollupName: Option[String]): Boolean = {
    analyses.last.limit.isEmpty ||
    rollupName.isDefined ||
      !analyses.exists { a =>
        a.where.isDefined ||
        a.groupBy.isDefined ||
        a.having.isDefined ||
        (a.orderBy.isDefined && a.orderBy.get.size > 1) ||
        a.search.isDefined ||
        a.distinct
      }
  }
}
