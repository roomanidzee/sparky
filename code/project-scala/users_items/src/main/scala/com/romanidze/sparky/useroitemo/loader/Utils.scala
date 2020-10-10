package com.romanidze.sparky.useroitemo.loader

import java.util.TimeZone
import java.text.SimpleDateFormat

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object Utils {

  val getTimeValue: UserDefinedFunction = udf { (timestamp: Long) =>
    val formatter = new SimpleDateFormat("yyyyMMdd")
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"))
    formatter.format(timestamp)
  }

  def getNormalizedValue(idColumn: Column): Column = {
    lower(regexp_replace(idColumn, "[\\s-]+", "_"))
  }

  def getJsonData(inputDir: String, dataType: String)(implicit spark: SparkSession): DataFrame = {

    val pivotColumn: String = s"${dataType}_column"
    val pivotPrefix: String = s"${dataType}_"

    spark.read
      .json(s"${inputDir}/${dataType}")
      .withColumn("utc_date", getTimeValue(col("timestamp")))
      .withColumn(pivotColumn, concat(lit(pivotPrefix), getNormalizedValue(col("item_id"))))
      .select(col("uid"), col("utc_date"), col(pivotColumn))
      .cache()

  }

  def getMaxDateValue(inputDF: DataFrame): String = {

    val dateValue: Row =
      inputDF
        .agg(max("utc_date").as("max_date"))
        .select(col("max_date"))
        .head()

    dateValue.getString(0)

  }

  def getMaxValue(first: String, second: String): String = {

    if (first > second) {
      first
    } else {
      second
    }

  }

}
