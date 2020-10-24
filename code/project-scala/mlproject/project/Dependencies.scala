
import sbt._

object Dependencies {

  private val spark: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark,
    "org.apache.spark" %% "spark-sql" % Versions.spark,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % Versions.spark,
    "org.apache.spark" %% "spark-mllib" % Versions.spark
  ).map(_ % Provided)

  val mainDeps: Seq[ModuleID] = spark

}
