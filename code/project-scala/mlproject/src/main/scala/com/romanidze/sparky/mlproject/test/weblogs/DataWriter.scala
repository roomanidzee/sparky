package com.romanidze.sparky.mlproject.test.weblogs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

class DataWriter(implicit spark: SparkSession) {

  def writeData(
    transformedDF: DataFrame,
    kafkaServers: String,
    topic: String,
    checkpointPath: String
  ): StreamingQuery = {

    transformedDF
      .select(col("uid"), col("predicted_label").as("gender_age"))
      .toJSON
      .writeStream
      .format("kafka")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .options(
        Map(
          "kafka.bootstrap.servers" -> kafkaServers,
          "topic"                   -> topic,
          "checkpointLocation"      -> checkpointPath
        )
      )
      .outputMode(OutputMode.Append())
      .start()

  }

}
