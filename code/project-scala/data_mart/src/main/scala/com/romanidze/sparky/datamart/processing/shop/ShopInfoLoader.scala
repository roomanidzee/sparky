package com.romanidze.sparky.datamart.processing.shop

import com.romanidze.sparky.datamart.config.ElasticSearchConfig
import com.romanidze.sparky.datamart.processing.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class ShopInfoLoader(config: ElasticSearchConfig)(implicit spark: SparkSession) {

  def getShopInfoDataFrame: DataFrame = {

    val esConfig =
      Map("pushdown" -> "true", "es.nodes" -> config.addresses)

    spark.read
      .format("org.elasticsearch.spark.sql")
      .options(esConfig)
      .load(config.indexName)
      .na
      .drop("all", Seq("uid"))
      .filter(col("event_type").isInCollection(Seq("buy", "view")))

  }

  def getAggregatedDataFrame(rawDF: DataFrame): DataFrame = {

    val calculatedDF = rawDF
      .groupBy(col("uid"), col("item_id"))
      .agg(count(col("event_type")).alias("action_count"))

    rawDF
      .join(calculatedDF, Seq("uid"), "inner")
      .select(col("uid"), col("action_count"), col("category"))
      .withColumn("shop_category", Utils.processCategory(col("category"), "shop_"))
      .orderBy(asc("uid"))

  }

}
