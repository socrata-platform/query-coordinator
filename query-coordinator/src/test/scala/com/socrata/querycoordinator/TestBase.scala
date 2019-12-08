package com.socrata.querycoordinator

import com.socrata.http.client.RequestBuilder
import com.socrata.soql.environment.TableName
import com.socrata.soql.typed._
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.PropertyConfigurator
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

abstract class TestBase extends FunSuite  with Matchers with ScalaCheckPropertyChecks {
  val config: Config = ConfigFactory.load().getConfig("com.socrata.query-coordinator")

  PropertyConfigurator.configure(Propertizer("log4j", config.getConfig("log4j")))

  val fakeRequestBuilder = RequestBuilder("")
}
