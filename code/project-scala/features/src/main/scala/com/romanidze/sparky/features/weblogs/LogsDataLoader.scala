package com.romanidze.sparky.features.weblogs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LogsDataLoader {

  def getLogsDF(implicit spark: SparkSession): DataFrame = {

    val logsRawDF: DataFrame = spark.read
      .json("/labs/laba03/weblogs.json")
      .select(col("uid"), explode(col("visits")).as('visit))
      .select(col("uid"), lower(col("visit.url")).as("url"), col("visit.timestamp").as("timestamp"))

    logsRawDF
      .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .select(col("uid"), col("timestamp"), col("domain"))
      .na
      .drop("any")
      .cache()

  }

}
