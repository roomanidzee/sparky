package com.romanidze.sparky.datamart.processing.shop

import com.holdenkarau.spark.testing.SharedSparkContext
import com.romanidze.sparky.datamart.config.ElasticSearchConfig
import com.romanidze.sparky.datamart.processing.SchemaProvider
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ShopInfoLoaderTest extends AnyWordSpec with Matchers with SharedSparkContext {

  "ShopInfoLoader" should {

    val esConfig = ElasticSearchConfig("localhost:9200", "visits")

    "correctly load information for shop" in {

      implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

      val shopInfoLoader = new ShopInfoLoader(esConfig)

      val shopDF: DataFrame = shopInfoLoader.getShopInfoDataFrame

      shopDF.schema shouldBe SchemaProvider.getShopInfoSchema

    }

    "collect aggregated dataframe" in {

      implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

      val shopInfoLoader = new ShopInfoLoader(esConfig)

      val shopInfoRDD: RDD[Row] = sc.parallelize(
        Seq(
          Row("test1", "buy", "test1_1", 1L, 1L, "test1"),
          Row("test2", "view", "test2_1", 2L, 2L, "test2"),
          Row("test3", "buy", "test3_3", 3L, 3L, "test3"),
          Row("test4", "view", "test4_4", 4L, 4L, "test4"),
          Row("test5", "buy", "test5_5", 5L, 5L, "test5")
        )
      )

      val shopInfoDF: DataFrame =
        spark.createDataFrame(shopInfoRDD, SchemaProvider.getShopInfoSchema)
      val aggregatedDF: DataFrame = shopInfoLoader.getAggregatedDataFrame(shopInfoDF)

      aggregatedDF.schema shouldBe SchemaProvider.getShopAggregatedSchema

      val collectedData: Array[Row] = aggregatedDF.collect()

      collectedData.length shouldBe 5

      val expectedData: Array[Row] = Array(
        Row("test1", 1, "test1", "shop_test1"),
        Row("test2", 1, "test2", "shop_test2"),
        Row("test3", 1, "test3", "shop_test3"),
        Row("test4", 1, "test4", "shop_test4"),
        Row("test5", 1, "test5", "shop_test5")
      )

      collectedData
        .zip(expectedData)
        .foreach(elem => {
          elem._1 shouldBe elem._2
        })

    }

  }

}
