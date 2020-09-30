import sbt._

object Dependencies {

  private val spark: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark,
    "org.apache.spark" %% "spark-sql" % Versions.spark
  ).map(_ % Provided)

  private val cassandra: Seq[ModuleID] = Seq(
    "com.datastax.spark" %% "spark-cassandra-connector" % Versions.cassandraConnector
  )

  private val elasticsearch: Seq[ModuleID] = Seq(
    "org.elasticsearch" %% "elasticsearch-spark-20" % Versions.elasticsearchConnector
  )

  private val postgresql: Seq[ModuleID] = Seq(
    "org.postgresql" % "postgresql" % Versions.postgresql
  )

  private val connectors: Seq[ModuleID] = cassandra.union(elasticsearch).union(postgresql)

  val mainDeps: Seq[ModuleID] = spark.union(connectors)

}
