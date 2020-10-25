import sbt._

object Dependencies {

  private val spark: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark,
    "org.apache.spark" %% "spark-sql" % Versions.spark,
    "org.apache.spark" %% "spark-mllib" % Versions.spark
  ).map(_ % Provided)

  private val elasticsearch: Seq[ModuleID] = Seq(
    "org.elasticsearch" %% "elasticsearch-spark-20" % Versions.elasticsearchConnector
  )

  val mainDeps: Seq[ModuleID] = spark.union(elasticsearch)

}
