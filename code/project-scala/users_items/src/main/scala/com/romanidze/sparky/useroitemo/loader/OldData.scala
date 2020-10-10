package com.romanidze.sparky.useroitemo.loader

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

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
        .drop("null", "view_column")
        .na.fill(0)

    val buyAggregatedDF: DataFrame =
      buyDF
        .drop(col("utc_date"))
        .groupBy(col("uid"))
        .pivot("buy_column")
        .agg(count(col("uid")))
        .drop("null", "buy_column")
        .na.fill(0)

    val joinedDF: DataFrame =
      viewAggregatedDF
        .join(buyAggregatedDF, Seq("uid"), "left")
        .drop(col("uid"))
        .drop("null")

    val newMatrix = oldMatrix
      .union(joinedDF)
      .groupBy(col("uid"))
      .sum()

    val renamedColumns: Array[Column] =
      newMatrix.columns
        .map(name => col(name).as(name.replaceAll("^sum\\(", "").replaceAll("\\)$", "")))

    newMatrix
      .select(renamedColumns: _*)
      .write
      .parquet(s"${outputDir}/${maxDateValue}")

    viewDF.unpersist()
    buyDF.unpersist()

  }

}
