package com.romanidze.sparky.datamart.processing.shop

import com.romanidze.sparky.datamart.config.ElasticSearchConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

class ShopInfoLoader(config: ElasticSearchConfig)(implicit spark: SparkSession) {

  def getShopInfoDataFrame: DataFrame = {

    val esConfig =
      Map("pushdown" -> "true", "es.nodes" -> config.addresses)

    spark.read
      .format("org.elasticsearch.spark.sql")
      .options(esConfig)
      .load(config.indexName)

  }

}
