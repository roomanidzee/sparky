package com.romanidze.sparky.useroitemo.loader

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object NewData {

  def load(inputDir: String, outputDir: String)(implicit spark: SparkSession): Unit = {

    val viewDF: DataFrame = Utils.getJsonData(inputDir, "view")

    viewDF.show(10)

    val buyDF: DataFrame = Utils.getJsonData(inputDir, "buy")

    buyDF.show(10)

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

    viewAggregatedDF.show(10)

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

    buyAggregatedDF.show(10)

    val joinedDF: DataFrame =
      viewAggregatedDF
        .join(buyAggregatedDF, Seq("uid"), "inner")
        .drop(col("uid"))

    joinedDF.show(10)

    joinedDF.write
      .parquet(s"${outputDir}/${maxDateValue}")

  }

}
