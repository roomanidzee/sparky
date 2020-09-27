package com.romanidze.sparky.datamart.processing.clients

import com.romanidze.sparky.datamart.config.CassandraConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class ClientInfoLoader(config: CassandraConfig)(implicit spark: SparkSession) {

  def getClientsDataFrame: DataFrame = {

    spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", config.keyspace)
      .option("table", config.table)
      .load()

  }

  def getJoinedDataFrame(
    clientsDF: DataFrame,
    weblogsDF: DataFrame,
    shopLogsDF: DataFrame
  ): DataFrame = {

    val logsJoinedDF: DataFrame = clientsDF.join(weblogsDF, Seq("uid"), "inner")

    val finalJoinedDF: DataFrame = logsJoinedDF
      .join(shopLogsDF, Seq("uid"), "inner")
      .orderBy(asc("uid"))

    val finalDF: DataFrame = finalJoinedDF
      .withColumn(
        "age_cat",
        when(col("age") >= 18 && col("age") <= 24, "18-24")
          .when(col("age") >= 25 && col("age") <= 34, "25-34")
          .when(col("age") >= 35 && col("age") <= 44, "35-44")
          .when(col("age") >= 45 && col("age") <= 54, "45-54")
          .when(col("age") >= 55, ">=55")
          .otherwise(0)
      )
      .select(
        col("uid"),
        col("gender"),
        col("age_cat"),
        col("url_count"),
        col("web_category"),
        col("action_count"),
        col("shop_category")
      )

    finalDF

  }

  def getPivotDataFrame(joinedDF: DataFrame): DataFrame = {

    val shopPivotDF: DataFrame =
      joinedDF
        .groupBy(col("uid"))
        .pivot("shop_category")
        .sum("action_count")

    val webPivotDF: DataFrame =
      joinedDF
        .groupBy(col("uid"))
        .pivot("web_category")
        .sum("url_count")

    val rawDF: DataFrame = joinedDF.select(col("uid"), col("gender"), col("age_cat"))

    val joinColumns: Seq[String] = Seq("uid")

    rawDF
      .join(shopPivotDF, joinColumns, "inner")
      .join(webPivotDF, joinColumns, "inner")

  }

}
