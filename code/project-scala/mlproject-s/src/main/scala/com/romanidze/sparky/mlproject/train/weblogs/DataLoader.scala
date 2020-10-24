package com.romanidze.sparky.mlproject.train.weblogs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DataLoader(implicit spark: SparkSession) {

  def load(datasetPath: String): DataFrame = {

    val logsRawDF: DataFrame = spark.read
      .json(datasetPath)
      .select(col("uid"), col("gender_age"), explode(col("visits")).as("visit"))
      .select(col("uid"), col("gender_age"), lower(col("visit.url")).as("url"))

    logsRawDF
      .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .select(col("uid"), col("gender_age"), col("domain"))
      .na
      .drop("any")
      .cache()

  }

  def convertDF(logsDF: DataFrame): DataFrame = {

    logsDF
      .groupBy(col("uid"), col("gender_age"))
      .agg(collect_list("domain").as("domains"))

  }

}
