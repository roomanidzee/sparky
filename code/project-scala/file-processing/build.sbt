import Dependencies._

name := "file-processing"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Dependencies.mainDeps ++ Dependencies.testDeps

testOptions in Test ++= Seq(
  Tests.Argument(TestFrameworks.ScalaTest, "-oT"),
  Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports")
),
