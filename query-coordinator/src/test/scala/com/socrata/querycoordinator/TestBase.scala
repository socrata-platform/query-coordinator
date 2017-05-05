package com.socrata.querycoordinator

import com.socrata.soql.environment.TableName
import com.socrata.soql.typed._
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.PropertyConfigurator
import org.scalatest.{BeforeAndAfterAll, FunSuite, ShouldMatchers}
import org.scalatest.prop.PropertyChecks

abstract class TestBase extends FunSuite  with ShouldMatchers  with PropertyChecks {
  val config: Config = ConfigFactory.load().getConfig("com.socrata.query-coordinator")

  PropertyConfigurator.configure(Propertizer("log4j", config.getConfig("log4j")))
}
