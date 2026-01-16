import Dependencies._

name := "secondary-selector"

libraryDependencies ++= Seq(
  rojomaJson,
  simpleArm,
  socrataThirdpartyUtils,
  sprayCaching,
  typesafeConfig,
  metricsScala,
  scalaLogging
)

disablePlugins(AssemblyPlugin)
