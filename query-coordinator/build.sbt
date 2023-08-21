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
  socrataHttpServerExt,
  socrataCuratorUtils,
  socrataThirdpartyUtils,
  socrataUtils,
  soqlStdlib,
  soqlUtils,
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
  scalaCheck,
  rollupMetrics
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)

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

assembly/assemblyJarName := s"${name.value}-assembly.jar"

assembly/assemblyOutputPath := target.value / (assembly/assemblyJarName).value
