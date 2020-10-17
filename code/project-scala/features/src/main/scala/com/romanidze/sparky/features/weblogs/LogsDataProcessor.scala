package com.romanidze.sparky.features.weblogs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class LogsDataProcessor(implicit spark: SparkSession) {

  def getTopVisitedSites(rawDF: DataFrame): DataFrame = {

    rawDF
      .drop("uid")
      .groupBy(col("domain"))
      .agg(count(col("domain").alias("domain_count")))
      .select(col("domain"), col("domain_count"))
      .orderBy(col("domain_count").desc)
      .limit(1000)
      .orderBy(col("domain").asc)

  }

}
