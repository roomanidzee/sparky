package com.romanidze.sparky.datamart.processing.shop

import com.holdenkarau.spark.testing.SharedSparkContext
import com.romanidze.sparky.datamart.config.ElasticSearchConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.Ignore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ShopInfoLoaderTest extends AnyFlatSpec with Matchers with SharedSparkContext {

  it should "correctly load information for shop" in {

    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

    val esConfig = ElasticSearchConfig("localhost:9200", "visits")

    val shopInfoLoader = new ShopInfoLoader(esConfig)

    val shopDF: DataFrame = shopInfoLoader.getShopInfoDataFrame

    shopDF.printSchema()

    spark.stop()

  }

}
