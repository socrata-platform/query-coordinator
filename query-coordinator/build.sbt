import scala.sys.process.Process

import Dependencies._

name := "query-coordinator"

enablePlugins(BuildInfoPlugin)

libraryDependencies ++= Seq(
  postgresql,
  c3p0,
  rojomaJson,
  simpleArm,
  socrataHttpClient,
  socrataHttpCuratorBroker,
  socrataHttpJetty,
  socrataCuratorUtils,
  socrataThirdpartyUtils,
  socrataUtils,
  soqlStdlib,
  protobuf, // these are optional dependencies of
  trove4j,  // soql-analysis
  typesafeConfig,
  metricsJetty,
  metricsGraphite,
  metricsJmx,
  metricsScala,
  slf4jLog4j12,
  scalaLogging,
  scalaTest,
  scalaCheck
)

lazy val gitSha = Process(Seq("git", "describe", "--always", "--dirty", "--long", "--abbrev=10")).!!.stripLineEnd

buildInfoKeys := Seq[BuildInfoKey](
  name,
  version,
  scalaVersion,
  sbtVersion,
  BuildInfoKey.action("buildTime") { System.currentTimeMillis },
  BuildInfoKey.action("revision") { gitSha })

buildInfoPackage := "com.socrata.querycoordinator"

buildInfoOptions += BuildInfoOption.ToMap

assembly/test := {}
