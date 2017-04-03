package com.socrata.querycoordinator.caching.cache.noop

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.querycoordinator.caching.cache._

import scala.concurrent.duration.FiniteDuration

class NoopCacheSessionProvider

object NoopCacheSessionProvider extends CacheSessionProvider with CacheCleanerProvider with CacheInit {
  protected val log = org.slf4j.LoggerFactory.getLogger(classOf[NoopCacheSessionProvider])

  override def open(rs: ResourceScope, dataset: String): CacheSession = NoopCacheSession

  override def cleaner(id: String, interval: FiniteDuration): CacheCleaner = NoopCacheCleaner

  override def init(): Unit = {}
}
