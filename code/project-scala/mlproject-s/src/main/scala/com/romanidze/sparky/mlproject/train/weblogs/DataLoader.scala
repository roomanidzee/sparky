package com.romanidze.sparky.mlproject.train.weblogs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DataLoader(implicit spark: SparkSession) {

  def load(datasetPath: String): DataFrame = {

    spark.read
      .json(datasetPath)
      .select(col("uid"), col("gender_age"), explode(col("visits")).as("visit"))
      .cache()

  }

}
