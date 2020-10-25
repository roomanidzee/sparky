name := "dashboard"

version := "1.0"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  Resolver.mavenCentral,
  Resolver.mavenLocal,
  Resolver.bintrayRepo("sbt-assembly", "maven")
)

libraryDependencies ++= Dependencies.mainDeps

scalafmtOnCompile := true

assemblyJarName in assembly := "dashboard_2.11-1.0.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
