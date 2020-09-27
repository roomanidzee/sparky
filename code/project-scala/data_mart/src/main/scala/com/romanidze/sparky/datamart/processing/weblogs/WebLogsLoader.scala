package com.romanidze.sparky.datamart.processing.weblogs

import com.romanidze.sparky.datamart.processing.SchemaProvider
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class WebLogsLoader(implicit spark: SparkSession) {

  def getWebLogsDataFrame(path: String): DataFrame = {

    spark.read
      .schema(SchemaProvider.getWebLogsSchema)
      .parquet(path)
      .na
      .drop("all")

  }

  private def processURL(rawURL: Column): Column = {

    val parsedURL: Column = callUDF("parse_url", rawURL, lit("HOST"))

    regexp_replace(parsedURL, lit("www."), lit(""))

  }

  def processLogsDataFrame(input: DataFrame): DataFrame = {

    input
      .select(col("uid"), explode(col("visits")).alias("exploded_visits"))
      .select(col("uid"), processURL(col("exploded_visits.url")).alias("url"))
      .groupBy(col("uid"), col("url"))
      .agg(count(col("url")).alias("url_count"))

  }

}
