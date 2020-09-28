name := "data_mart"

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

scalacOptions := Seq("-unchecked", "-feature", "-deprecation", "-encoding", "utf8", "-target:jvm-1.8")
scalafmtOnCompile := true

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M",  "-XX:+CMSClassUnloadingEnabled")

assemblyJarName in assembly := "data_mart_2.11-1.0.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

test in assembly := {}