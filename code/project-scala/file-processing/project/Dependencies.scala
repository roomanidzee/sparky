
import sbt._

object Dependencies {

  private val scalaTest: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalaTest,
    "org.scalactic" %% "scalactic" % Versions.scalaTest
  ).map(_ % Test)

  private val flexmark: Seq[ModuleID] =
    Seq("com.vladsch.flexmark" % "flexmark-all" % Versions.flexMark).map(_ % Test)

  private val betterFiles: Seq[ModuleID] = Seq(
    "com.github.pathikrit" %% "better-files" % Versions.betterFiles
  )

  private val scallop: Seq[ModuleID] = Seq(
    "org.rogach" %% "scallop" % Versions.scallop
  )

  private val tethys: Seq[ModuleID] = Seq(
    "com.tethys-json" %% "tethys-core" % Versions.tethys,
    "com.tethys-json" %% "tethys-jackson" % Versions.tethys
  )

  val mainDeps: Seq[ModuleID] = betterFiles.union(scallop).union(tethys)
  val testDeps: Seq[ModuleID] = scalaTest.union(flexmark)

}
