import sbt._

object Dependencies {

  private val scalaTest: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalaTest,
    "org.scalactic" %% "scalactic" % Versions.scalaTest
  )

  private val flexmark: Seq[ModuleID] =
    Seq("com.vladsch.flexmark" % "flexmark-all" % Versions.flexMark)

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

  private val pureConfig: Seq[ModuleID] = Seq(
    "com.github.pureconfig" %% "pureconfig" % Versions.pureconfig
  )

  private val sparkTesting: Seq[ModuleID] = Seq(
    "com.holdenkarau" %% "spark-testing-base" % Versions.sparkTestingBase
  )

  val mainDeps: Seq[ModuleID] = spark.union(connectors).union(pureConfig)

  val testDeps: Seq[ModuleID] =
     scalaTest.union(flexmark)
              .union(sparkTesting).map(_ % Test)

}
