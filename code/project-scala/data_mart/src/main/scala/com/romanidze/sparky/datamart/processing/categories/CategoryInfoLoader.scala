package com.romanidze.sparky.datamart.processing.categories

import com.romanidze.sparky.datamart.config.PostgreSQLConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

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

}
