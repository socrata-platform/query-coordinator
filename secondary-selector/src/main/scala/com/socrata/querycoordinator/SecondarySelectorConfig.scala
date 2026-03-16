package com.socrata.querycoordinator

import scala.collection.JavaConverters._
import com.socrata.thirdparty.typesafeconfig.ConfigClass

import scala.concurrent.duration.Duration

trait SecondarySelectorConfig extends ConfigClass {

  val allSecondaryInstanceNames: Seq[String] = getStringList("all-secondary-instance-names")

  val secondaryDiscoveryExpirationMillis: Long = getDuration("secondary-discovery-expiration").toMillis

  val datasetMaxNopeCount: Int = getInt("dataset-max-nope-count")

  val maxCacheEntries: Int = getInt("secondary-selector-max-cache-entries")

  // new cache settings

  val absentInterval = getDuration("absent-interval")
  val absentBound = getDuration("absent-bound")

  val unknownInterval = getDuration("unknown-interval")
  val unknownBound = getDuration("unknown-bound")
}
