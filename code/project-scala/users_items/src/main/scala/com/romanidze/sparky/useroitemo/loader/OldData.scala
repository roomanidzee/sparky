package com.romanidze.sparky.useroitemo.loader

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object OldData {

  def load(inputDir: String, outputDir: String)(implicit spark: SparkSession): Unit = {

    val viewDF: DataFrame = Utils.getJsonData(inputDir, "view")
    val buyDF: DataFrame = Utils.getJsonData(inputDir, "buy")

    val oldMatrix: DataFrame =
      spark.read
        .parquet(s"${outputDir}/*")
        .cache()

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
      .parquet(s"${outputDir}/20200430")

    viewDF.unpersist()
    buyDF.unpersist()

  }

}
