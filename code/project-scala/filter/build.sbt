name := "filter"

version := "1.0"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  Resolver.mavenCentral,
  Resolver.mavenLocal,
  Resolver.bintrayRepo("sbt-assembly", "maven")
)

libraryDependencies ++= Dependencies.mainDeps
