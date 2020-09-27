package com.romanidze.sparky.datamart.processing.categories

import com.holdenkarau.spark.testing.SharedSparkContext
import com.romanidze.sparky.datamart.config.{DataInfo, PostgreSQLConfig}
import com.romanidze.sparky.datamart.processing.SchemaProvider
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CategoriesInfoLoaderTest extends AnyWordSpec with Matchers with SharedSparkContext {

  "CategoriesInfoLoader" should {

    val psqlConfig = PostgreSQLConfig(
      "localhost",
      "5432",
      "default_user",
      "default_pass",
      DataInfo("default_db", "domain_cats"),
      DataInfo("", "")
    )

    "correctly load information for categories" in {

      implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

      val categoryInfoLoader = new CategoryInfoLoader(psqlConfig)

      val categoriesDF: DataFrame = categoryInfoLoader.getCategoriesDataFrame

      categoriesDF.schema shouldBe SchemaProvider.getCategoriesSchema

      categoriesDF.show(1, 200, vertical = true)
      categoriesDF.count() > 0 shouldBe true

    }

    "join information from logs and database" in {

      implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()
      val categoryInfoLoader = new CategoryInfoLoader(psqlConfig)

      val webLogsRDD: RDD[Row] = sc.parallelize(
        Seq(
          Row("test1", "test1.ru", 1L),
          Row("test2", "test.test2.ru", 2L),
          Row("test3", "http", 3L),
          Row("test4", "none", 4L),
          Row("test5", "test5.ru", 5L)
        )
      )

      val webLogsDF: DataFrame =
        spark.createDataFrame(webLogsRDD, SchemaProvider.getWebLogsAggregatedSchema)

      val categoriesRDD: RDD[Row] = sc.parallelize(
        Seq(
          Row("test1", "test1"),
          Row("test2", "test2"),
          Row("http", "test3"),
          Row("test4", "test4"),
          Row("test5", "test5")
        )
      )

      val categoriesDF: DataFrame =
        spark.createDataFrame(categoriesRDD, SchemaProvider.getCategoriesSchema)

      val joinedDF: DataFrame = categoryInfoLoader.getJoinedDF(webLogsDF, categoriesDF)

      joinedDF.schema shouldBe SchemaProvider.getCategoriesAggregatedSchema

      val collectedData: Array[Row] = joinedDF.collect()

      val expectedData: Array[Row] = Array(
        Row("test1", 1, "test1", "web_test1"),
        Row("test2", 2, "test2", "web_test2"),
        Row("test3", 3, "test3", "web_test3"),
        Row("test5", 5, "test5", "web_test5")
      )

      collectedData
        .zip(expectedData)
        .foreach(elem => {
          elem._1 shouldBe elem._2
        })

    }

  }

}
