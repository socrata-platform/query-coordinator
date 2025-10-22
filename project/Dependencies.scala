import sbt._

object Dependencies {

  object versions {
    val postgresql = "42.2.5"
    val c3p0 = "0.9.5.2"
    val socrataHttpVersion = "3.16.2"
    val rojomaJson = "3.13.0"
    val socrataCuratorUtils = "1.2.0"
    val socrataThirdpartyUtils = "5.0.0"
    val socrataUtils = "0.11.0"
    val soqlStdlib = "4.14.89"
    val protobuf = "2.4.1"
    val trove4j = "3.0.3"
    val sprayCaching = "1.2.2"
    val typesafeConfig = "1.2.1"
    val metrics = "4.1.2"
    val metricsScala = "4.1.1"
    val scalaLogging = "3.9.2"
    val slf4j =  "1.7.7"
    val scalaTest = "3.0.8"
    val scalaCheck = "1.14.0"
    val simpleArm = "2.3.3"
    val rollupMetrics = "3.5"
  }

  val postgresql = "org.postgresql" % "postgresql" % versions.postgresql
  val c3p0 = "com.mchange" % "c3p0" % versions.c3p0
  val rojomaJson = "com.rojoma" %% "rojoma-json-v3" % versions.rojomaJson
  val socrataHttpClient = "com.socrata" %% "socrata-http-client" % versions.socrataHttpVersion
  val socrataHttpCuratorBroker = "com.socrata" %% "socrata-http-curator-broker" % versions.socrataHttpVersion exclude("org.slf4j", "slf4j-simple") exclude ("org.jboss.netty", "netty" /* see ZOOKEEPER-1681 */)
  val socrataHttpJetty = "com.socrata" %% "socrata-http-jetty" % versions.socrataHttpVersion
  val socrataHttpServerExt = "com.socrata" %% "socrata-http-server-ext" % versions.socrataHttpVersion
  val socrataCuratorUtils = "com.socrata" %% "socrata-curator-utils" % versions.socrataCuratorUtils
  val socrataThirdpartyUtils = "com.socrata" %% "socrata-thirdparty-utils" % versions.socrataThirdpartyUtils
  val socrataUtils = "com.socrata" %% "socrata-utils" % versions.socrataUtils

  val soqlStdlib = "com.socrata" %% "soql-stdlib" % versions.soqlStdlib exclude ("javax.media", "jai_core")
  val soqlUtils = "com.socrata" %% "soql-utils" % versions.soqlStdlib exclude ("javax.media", "jai_core")
  val protobuf = "com.google.protobuf" % "protobuf-java" % versions.protobuf  // these are optional deps
  val trove4j = "net.sf.trove4j" % "trove4j" % versions.trove4j               // of soql-analysis

  val sprayCaching = "io.spray" % "spray-caching" % versions.sprayCaching
  val typesafeConfig = "com.typesafe" % "config" % versions.typesafeConfig

  val metricsJetty = "io.dropwizard.metrics" % "metrics-jetty9" % versions.metrics
  val metricsGraphite = "io.dropwizard.metrics" % "metrics-graphite" % versions.metrics
  val metricsJmx = "io.dropwizard.metrics" % "metrics-jmx" % versions.metrics
  val metricsScala = "nl.grons" %% "metrics4-scala" % versions.metricsScala

  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % versions.scalaLogging
  val slf4jLog4j12 = "org.slf4j" % "slf4j-log4j12" % versions.slf4j

  val simpleArm = "com.rojoma" %% "simple-arm-v2" % versions.simpleArm

  val scalaTest = "org.scalatest" %% "scalatest" % versions.scalaTest % "test"
  val scalaCheck = "org.scalacheck" %% "scalacheck" % versions.scalaCheck % "test"

  val rollupMetrics = "com.socrata" %% "rollup-metrics" % versions.rollupMetrics
}
