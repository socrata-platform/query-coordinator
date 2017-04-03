package com.socrata.querycoordinator.caching.cache

import scala.concurrent.duration.FiniteDuration

trait CacheCleanerProvider {
  def cleaner(id: String, interval: FiniteDuration): CacheCleaner
}
