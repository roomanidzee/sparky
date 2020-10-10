package com.romanidze.sparky.useroitemo.loader

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OldData {

  def load(inputDir: String, outputDir: String)(implicit spark: SparkSession): Unit = {

    val viewDF: DataFrame = Utils.getJsonData(inputDir, "view")
    val buyDF: DataFrame = Utils.getJsonData(inputDir, "buy")

    val oldMatrix: DataFrame =
      spark.read
        .parquet(s"${outputDir}")
        .cache()

    val viewDateValue: String = Utils.getMaxDateValue(viewDF)
    val buyDateValue: String = Utils.getMaxDateValue(buyDF)

    val maxDateValue: String = Utils.getMaxValue(viewDateValue, buyDateValue)

    val viewAggregatedDF: DataFrame =
      viewDF
        .drop(col("utc_date"))
        .groupBy(col("uid"))
        .pivot("view_column")
        .agg(count(col("uid")))
        .drop("null")
        .na
        .fill(0)
        .drop(col("view_column"))

    val buyAggregatedDF: DataFrame =
      buyDF
        .drop(col("utc_date"))
        .groupBy(col("uid"))
        .pivot("buy_column")
        .agg(count(col("uid")))
        .drop("null")
        .na
        .fill(0)
        .drop(col("buy_column"))

    val joinedDF: DataFrame =
      viewAggregatedDF
        .join(buyAggregatedDF, Seq("uid"), "inner")
        .drop(col("uid"))

    val newMatrix = oldMatrix
      .union(joinedDF)
      .groupBy(col("uid"))
      .sum()

    newMatrix.write
      .parquet(s"${outputDir}/${maxDateValue}")

    viewDF.unpersist()
    buyDF.unpersist()

  }

}
