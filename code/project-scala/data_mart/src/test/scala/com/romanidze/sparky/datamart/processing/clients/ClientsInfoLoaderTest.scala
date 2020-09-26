package com.romanidze.sparky.datamart.processing.clients

import com.holdenkarau.spark.testing.SharedSparkContext
import com.romanidze.sparky.datamart.config.CassandraConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// нестабильный тест из-за коннектора, может сломаться или зависнуть по рандому
@Ignore
class ClientsInfoLoaderTest extends AnyFlatSpec with Matchers with SharedSparkContext {

  it should "correctly load information for clients" in {

    val cassandraConfig = CassandraConfig("localhost", "9042", "labdata", "clients")

    implicit lazy val spark: SparkSession =
      SparkSession.builder
        .config(sc.getConf)
        .config("spark.cassandra.connection.host", "localhost")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.cassandra.auth.username", "cassandra")
        .config("spark.cassandra.auth.password", "cassandra")
        .getOrCreate()

    val clientsInfoLoader = new ClientInfoLoader(cassandraConfig)

    val clientsDF: DataFrame = clientsInfoLoader.getClientsDataFrame

    clientsDF.printSchema()
    clientsDF.show(1, 200, vertical = true)

    clientsDF.count() > 0 shouldBe true

    spark.stop()

  }

}
