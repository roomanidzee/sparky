
import sbt._

object Dependencies {

  private val betterFiles: Seq[ModuleID] = Seq(
    "com.github.pathikrit" %% "better-files" % Versions.betterFiles
  )

  private val scallop: Seq[ModuleID] = Seq(
    "org.rogach" %% "scallop" % Versions.scallop
  )

  private val specs2: Seq[ModuleID]= Seq(
    "org.specs2" %% "specs2-core" % Versions.specs2 % Test
  )

  val mainDeps: Seq[ModuleID] = betterFiles.union(scallop)
  val testDeps: Seq[ModuleID] = specs2

}
