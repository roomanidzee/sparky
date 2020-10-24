name := "mlproject-s"

version := "1.0"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  Resolver.mavenCentral,
  Resolver.mavenLocal
)

libraryDependencies ++= Dependencies.mainDeps
scalafmtOnCompile := true
