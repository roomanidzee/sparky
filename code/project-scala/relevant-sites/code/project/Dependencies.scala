
import sbt._

object Dependencies {

  private val scalaTest: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalaTest,
    "org.scalactic" %% "scalactic" % Versions.scalaTest
  ).map(_ % Test)

  private val flexmark: Seq[ModuleID] =
    Seq("com.vladsch.flexmark" % "flexmark-all" % Versions.flexMark).map(_ % Test)

  private val spark: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark,
    "org.apache.spark" %% "spark-sql" % Versions.spark
  ).map(_ % Provided)

  val mainDeps: Seq[ModuleID] = spark

  val testDeps: Seq[ModuleID] = scalaTest.union(flexmark)

}
