
import sbt._

object Dependencies {

  private val spark: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark,
    "org.apache.spark" %% "spark-sql" % Versions.spark
  ).map(_ % Provided)

  val mainDeps: Seq[ModuleID] = spark

}
