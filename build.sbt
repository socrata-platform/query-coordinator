ThisBuild / organization := "com.socrata"

ThisBuild / scalaVersion := "2.12.8"

ThisBuild / resolvers += "socrata releases" at "https://repo.socrata.com/artifactory/libs-release"

ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature")

ThisBuild / evictionErrorLevel := Level.Warn

val secondarySelector = project in file("secondary-selector")

val queryCoordinator = (project in file("query-coordinator")).
  dependsOn(secondarySelector)

disablePlugins(AssemblyPlugin)

releaseProcess -= ReleaseTransformations.publishArtifacts
