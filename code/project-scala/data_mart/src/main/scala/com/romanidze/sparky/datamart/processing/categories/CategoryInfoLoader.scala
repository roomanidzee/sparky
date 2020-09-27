package com.romanidze.sparky.datamart.processing.categories

import com.romanidze.sparky.datamart.config.PostgreSQLConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class CategoryInfoLoader(config: PostgreSQLConfig)(implicit spark: SparkSession) {

  def getCategoriesDataFrame: DataFrame = {

    val jdbcURL =
      s"jdbc:postgresql://${config.host}:${config.port}/${config.source.database}?user=${config.user}&password=${config.password}"

    spark.read
      .format("jdbc")
      .option("url", jdbcURL)
      .option("dbtable", config.source.table)
      .load()

  }

  def getJoinedDF(webLogsDF: DataFrame, categoriesDF: DataFrame): DataFrame = {

    webLogsDF
      .join(categoriesDF, when(col("url").contains(col("domain")), true), "left")
      .select(col("uid"), col("url_count"), col("category"))
      .withColumn("web_category", concat(lit("web_"), col("category")))
      .na
      .drop("all", Seq("category", "web_category"))

  }

}
