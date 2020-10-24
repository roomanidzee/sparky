package com.romanidze.sparky.mlproject.test.weblogs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DataLoader(implicit spark: SparkSession) {

  def load(kafkaServers: String, topic: String): DataFrame = {

    spark.readStream
      .format("kafka")
      .options(
        Map(
          "kafka.bootstrap.servers" -> kafkaServers,
          "subscribe"               -> topic,
          "maxOffsetsPerTrigger"    -> "5000",
          "startingOffsets"         -> "earliest"
        )
      )
      .load

  }

  def convertToSchemaDF(rawDF: DataFrame): DataFrame = {

    val rawStringDF: DataFrame = rawDF.selectExpr("CAST(value AS STRING)")

    rawStringDF
      .select(from_json(col("value"), SchemaProvider.testDatasetSchema).as("data"))
      .select("data.*")

  }

  def getMLDF(schemaDF: DataFrame): DataFrame = {

    val rawDF: DataFrame =
      schemaDF
        .select(col("uid"), explode(col("visits")).as("visit"))
        .select(col("uid"), lower(col("visit.url")).as("url"))

    val logsDF: DataFrame =
      rawDF
        .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
        .withColumn("domain", regexp_replace(col("host"), "www.", ""))
        .select(col("uid"), col("domain"))

    logsDF
      .groupBy(col("uid"))
      .agg(collect_list("domain").as("domains"))

  }

}
