
import Dependencies._

name := "relevant-sites"

version := "0.1"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  Resolver.mavenCentral,
  Resolver.mavenLocal,
  Resolver.bintrayRepo("sbt-assembly", "maven")
)

libraryDependencies ++= Dependencies.mainDeps ++ Dependencies.testDeps

testOptions in Test ++= Seq(
  Tests.Argument(TestFrameworks.ScalaTest, "-oT"),
  Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports")
)
