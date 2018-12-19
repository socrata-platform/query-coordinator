resolvers ++= Seq(
  "socrata releases" at "https://repo.socrata.com/artifactory/libs-release",
  Resolver.url("socrata ivy releases", url("https://repo.socrata.com/artifactory/ivy-libs-release-local"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.6.8")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.4.0")
