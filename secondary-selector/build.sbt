import Dependencies._

name := "secondary-selector"

libraryDependencies ++= Seq(
  opentelemetryApi,
  rojomaJson,
  simpleArm,
  socrataThirdpartyUtils,
  sprayCaching,
  typesafeConfig,
  metricsScala,
  scalaLogging
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)

disablePlugins(AssemblyPlugin)
