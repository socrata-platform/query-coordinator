import Dependencies._

name := "secondary-selector"

libraryDependencies ++= Seq(
  socrataThirdpartyUtils,
  sprayCaching,
  typesafeConfig,
  metricsScala,
  scalaLogging
)

disablePlugins(AssemblyPlugin)
