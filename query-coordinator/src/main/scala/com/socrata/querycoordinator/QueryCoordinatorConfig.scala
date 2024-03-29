package com.socrata.querycoordinator

import java.util.concurrent.TimeUnit

import com.socrata.http.server.livenesscheck.LivenessCheckConfig
import com.socrata.curator.{CuratorConfig, DiscoveryConfig}
import com.socrata.querycoordinator.caching.cache.config.CacheConfig
import com.socrata.thirdparty.metrics.MetricsOptions
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.config.Config

import scala.concurrent.duration._

class QueryCoordinatorConfig(config: Config, root: String)
  extends ConfigClass(config, root) with SecondarySelectorConfig {
  val log4j = getRawConfig("log4j")
  val curator = new CuratorConfig(config, path("curator"))
  val discovery = new DiscoveryConfig(config, path("service-advertisement"))
  val livenessCheck = new LivenessCheckConfig(config, path("liveness-check"))
  val network = new NetworkConfig(config, path("network"))
  val metrics = MetricsOptions(config.getConfig(path("metrics")))

  val connectTimeout = config.getDuration(path("connect-timeout"), TimeUnit.MILLISECONDS).millis
  val schemaTimeout = config.getDuration(path("get-schema-timeout"), TimeUnit.MILLISECONDS).millis
  val receiveTimeout = config.getDuration(path("query-timeout"), TimeUnit.MILLISECONDS).millis // http query (not db query)
  val maxDbQueryTimeout = config.getDuration(path("max-db-query-timeout"), TimeUnit.MILLISECONDS).millis
  val maxRows = optionally(getInt("max-rows"))
  val defaultRowsLimit = getInt("default-rows-limit")

  val cache = getConfig("cache", new CacheConfig(_, _))

  val threadpool = getRawConfig("threadpool")
}
