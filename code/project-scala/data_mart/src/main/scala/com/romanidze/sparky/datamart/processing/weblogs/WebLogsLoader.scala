package com.romanidze.sparky.datamart.processing.weblogs

import com.romanidze.sparky.datamart.processing.SchemaProvider
import org.apache.spark.sql.{DataFrame, SparkSession}

class WebLogsLoader(implicit spark: SparkSession) {

  def getWebLogsDataFrame(path: String): DataFrame = {

    spark.read
      .schema(SchemaProvider.getWebLogsSchema)
      .parquet(path)
      .na
      .drop("all")

  }

}
