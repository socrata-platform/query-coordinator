package com.socrata.querycoordinator

import scala.collection.JavaConverters._
import com.socrata.thirdparty.typesafeconfig.ConfigClass

import scala.concurrent.duration.Duration

trait SecondarySelectorConfig extends ConfigClass {

  val allSecondaryInstanceNames: Seq[String] = getStringList("all-secondary-instance-names")

  val mirrors: Map[String, List[String]] = getObjectOf("mirrors", (config, root) => config.getStringList(root).asScala).mapValues(_.toList)

  val secondaryDiscoveryExpirationMillis: Long = getDuration("secondary-discovery-expiration").toMillis

  val datasetMaxNopeCount: Int = getInt("dataset-max-nope-count")

  val maxCacheEntries: Int = getInt("secondary-selector-max-cache-entries")
}
