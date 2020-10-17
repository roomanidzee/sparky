name := "features"

version := "1.0"

resolvers ++= Seq(
  Resolver.mavenCentral,
  Resolver.mavenLocal
)

scalaVersion := "2.11.12"

libraryDependencies ++= Dependencies.mainDeps
scalafmtOnCompile := true
