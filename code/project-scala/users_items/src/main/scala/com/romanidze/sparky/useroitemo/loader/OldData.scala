package com.romanidze.sparky.useroitemo.loader

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object OldData {

  def load(inputDir: String, outputDir: String)(implicit spark: SparkSession): Unit = {

    val viewDF: DataFrame = Utils.getJsonData(inputDir, "view")
    val buyDF: DataFrame = Utils.getJsonData(inputDir, "buy")

    val viewDateValue: String = Utils.getMaxDateValue(viewDF)
    val buyDateValue: String = Utils.getMaxDateValue(buyDF)

    val maxDateValue: String = Utils.getMaxValue(viewDateValue, buyDateValue)

    val viewAggregatedDF: DataFrame =
      viewDF
        .drop(col("utc_date"))
        .groupBy(col("uid"))
        .pivot("view_column")
        .count()
        .drop("view_column")

    val buyAggregatedDF: DataFrame =
      buyDF
        .drop(col("utc_date"))
        .groupBy(col("uid"))
        .pivot("buy_column")
        .count()
        .drop("buy_column")

    val joinedDF: DataFrame =
      viewAggregatedDF
        .join(buyAggregatedDF, Seq("uid"), "left")
        .drop(col("uid"))
        .na
        .fill(0)

    joinedDF.write
      .parquet(s"${outputDir}/${maxDateValue}")

    viewDF.unpersist()
    buyDF.unpersist()

  }

}
