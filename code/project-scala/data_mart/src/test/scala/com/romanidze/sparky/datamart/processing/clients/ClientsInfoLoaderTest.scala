package com.romanidze.sparky.datamart.processing.clients

import com.holdenkarau.spark.testing.SharedSparkContext
import com.romanidze.sparky.datamart.config.CassandraConfig
import com.romanidze.sparky.datamart.processing.SchemaProvider
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.Ignore
import org.scalatest.matchers.should.Matchers
import org.scalatest.refspec.RefSpec

class ClientsInfoLoaderTest extends RefSpec with Matchers with SharedSparkContext {

  val cassandraConfig = CassandraConfig("localhost", "9042", "labdata", "clients")

  implicit lazy val spark: SparkSession =
    SparkSession.builder
      .config(sc.getConf)
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.cassandra.auth.username", "cassandra")
      .config("spark.cassandra.auth.password", "cassandra")
      .getOrCreate()

  // нестабильный тест из-за коннектора, может сломаться или зависнуть по рандому
  @Ignore
  def `ClientsInfoLoader should correctly load information for clients`() {

    val clientsInfoLoader = new ClientInfoLoader(cassandraConfig)

    val clientsDF: DataFrame = clientsInfoLoader.getClientsDataFrame

    clientsDF.printSchema()
    clientsDF.show(1, 200, vertical = true)

    clientsDF.count() > 0 shouldBe true

    spark.stop()

  }

  def `ClientsInfoLoader should retrieve joined dataframe and create pivot dataframe`(): Unit = {

    val clientsInfoLoader = new ClientInfoLoader(cassandraConfig)

    val categoriesRDD: RDD[Row] = sc.parallelize(
      Seq(
        Row("test1", 1L, "test1", "web_test1"),
        Row("test2", 1L, "test2", "web_test2"),
        Row("test3", 1L, "test3", "web_test3"),
        Row("test4", 1L, "test4", "web_test4"),
        Row("test5", 1L, "test5", "web_test5")
      )
    )

    val categoriesDF: DataFrame =
      spark.createDataFrame(categoriesRDD, SchemaProvider.getCategoriesAggregatedSchema)

    val shopRDD: RDD[Row] = sc.parallelize(
      Seq(
        Row("test1", 1L, "test1", "shop_test1"),
        Row("test2", 1L, "test2", "shop_test2"),
        Row("test3", 1L, "test3", "shop_test3"),
        Row("test4", 1L, "test4", "shop_test4"),
        Row("test5", 1L, "test5", "shop_test5")
      )
    )

    val shopDF: DataFrame = spark.createDataFrame(shopRDD, SchemaProvider.getShopAggregatedSchema)

    val clientsRDD: RDD[Row] = sc.parallelize(
      Seq(
        Row("test1", "M", 18),
        Row("test2", "F", 26),
        Row("test3", "M", 37),
        Row("test4", "F", 48),
        Row("test5", "M", 58)
      )
    )

    val clientsDF: DataFrame = spark.createDataFrame(clientsRDD, SchemaProvider.getClientInfoSchema)

    val joinedDF: DataFrame = clientsInfoLoader.getJoinedDataFrame(clientsDF, categoriesDF, shopDF)

    val collectedJoinedData: Array[Row] = joinedDF.collect()

    collectedJoinedData.length shouldBe 5

    val expectedJoinedData: Array[Row] = Array(
      Row("test1", "M", "18-24", 1, "web_test1", 1, "shop_test1"),
      Row("test2", "F", "25-34", 1, "web_test2", 1, "shop_test2"),
      Row("test3", "M", "35-44", 1, "web_test3", 1, "shop_test3"),
      Row("test4", "F", "45-54", 1, "web_test4", 1, "shop_test4"),
      Row("test5", "M", ">=55", 1, "web_test5", 1, "shop_test5")
    )

    collectedJoinedData
      .zip(expectedJoinedData)
      .foreach(elem => {
        elem._1 shouldBe elem._2
      })

    val expectedPivotSchema = StructType(
      Seq(
        StructField("uid", DataTypes.StringType, nullable = true),
        StructField("gender", DataTypes.StringType, nullable = true),
        StructField("age_cat", DataTypes.StringType, nullable = false),
        StructField("shop_test1", DataTypes.LongType, nullable = true),
        StructField("shop_test2", DataTypes.LongType, nullable = true),
        StructField("shop_test3", DataTypes.LongType, nullable = true),
        StructField("shop_test4", DataTypes.LongType, nullable = true),
        StructField("shop_test5", DataTypes.LongType, nullable = true),
        StructField("web_test1", DataTypes.LongType, nullable = true),
        StructField("web_test2", DataTypes.LongType, nullable = true),
        StructField("web_test3", DataTypes.LongType, nullable = true),
        StructField("web_test4", DataTypes.LongType, nullable = true),
        StructField("web_test5", DataTypes.LongType, nullable = true)
      )
    )

    val pivotDF: DataFrame = clientsInfoLoader.getPivotDataFrame(joinedDF)

    pivotDF.schema shouldBe expectedPivotSchema

  }

}
