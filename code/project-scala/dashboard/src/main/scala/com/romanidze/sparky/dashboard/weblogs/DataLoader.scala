package com.romanidze.sparky.dashboard.weblogs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DataLoader(implicit spark: SparkSession) {

  def load(datasetPath: String): DataFrame = {

    val logsRawDF: DataFrame = spark.read
      .json(datasetPath)
      .select(col("uid"), col("date"), explode(col("visits")).as("visit"))
      .select(col("uid"), col("date"), lower(col("visit.url")).as("url"))

    logsRawDF
      .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .select(col("uid"), col("date"), col("domain"))
      .na
      .drop("any")
      .cache()

  }

  def convertDF(logsDF: DataFrame): DataFrame = {

    logsDF
      .groupBy(col("uid"), col("date"))
      .agg(collect_list("domain").as("domains"))

  }

}
