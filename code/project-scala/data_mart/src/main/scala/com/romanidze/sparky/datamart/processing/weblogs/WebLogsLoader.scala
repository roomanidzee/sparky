package com.romanidze.sparky.datamart.processing.weblogs

import com.romanidze.sparky.datamart.processing.{SchemaProvider, Utils}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class WebLogsLoader(implicit spark: SparkSession) {

  def getWebLogsDataFrame(path: String): DataFrame = {

    spark.read
      .schema(SchemaProvider.getWebLogsSchema)
      .parquet(path)
      .na
      .drop("all", Seq("uid"))

  }

  def processLogsDataFrame(input: DataFrame): DataFrame = {

    input
      .select(col("uid"), explode(col("visits")).alias("exploded_visits"))
      .select(col("uid"), Utils.processURL(col("exploded_visits.url")).alias("url"))
      .groupBy(col("uid"), col("url"))
      .agg(count(col("url")).alias("url_count"))

  }

}
