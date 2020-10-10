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

    val viewChangedDF: DataFrame =
      viewDF
        .withColumn("changed_column", col("view_column"))
        .drop("utc_date", "view_column")

    val buyChangedDF: DataFrame =
      buyDF
        .withColumn("changed_column", col("buy_column"))
        .drop("utc_date", "buy_column")

    val oldColumns: Seq[String] = oldMatrix.columns.toList.filter(_ != "uid")
    val columnsForStack: String = oldColumns.map(s => "\"" + s + "\", " + s).mkString(", ")
    val stackExpression: String =
      "stack(" + oldColumns.length.toString + ", " + columnsForStack + ") as (changed_column, value)"

    val changedOldMatrix: DataFrame = oldMatrix.selectExpr("uid", stackExpression)
    val finalOldMatrix: DataFrame = changedOldMatrix.select(col("uid"), col("changed_column"))

    val unionDF: DataFrame =
      viewChangedDF
        .union(buyChangedDF)
        .union(finalOldMatrix)

    val finalDF: DataFrame =
      unionDF
        .groupBy(col("uid"))
        .pivot("changed_column")
        .count()
        .drop("changed_column")

    val possibleNullColumns = finalDF.columns.filter(_ != "uid")

    finalDF.na
      .fill(0, possibleNullColumns)
      .write
      .parquet(s"${outputDir}/${maxDateValue}")

    oldMatrix.unpersist()
    viewDF.unpersist()
    buyDF.unpersist()

  }

}
