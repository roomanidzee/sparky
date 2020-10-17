package com.romanidze.sparky.features.weblogs.processing

import com.romanidze.sparky.features.weblogs.Utils
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

class TimeProcessing(implicit spark: SparkSession) {

  private val dayLiterals: Seq[String] = Seq("mon", "tue", "wed", "thu", "fri", "sat", "sun")

  private val hourLiterals: Seq[Int] = 0 to 23

  private def getCondition(columnName: String, aliasColumn: String, filterValue: Int): Column = {
    count(when(col(columnName) === filterValue, true).as(aliasColumn))
  }

  def getTimedDF(logsDF: DataFrame): DataFrame = {

    val dayColumns: Seq[Column] =
      dayLiterals
        .map(elem => ("day_value", s"web_day_$elem", dayLiterals.indexOf(elem) + 1))
        .map(elem1 => getCondition(elem1._1, elem1._2, elem1._3))

    val hourColumns: Seq[Column] =
      hourLiterals
        .map(elem => ("hour_value", s"web_hour_$elem", elem))
        .map(elem1 => getCondition(elem1._1, elem1._2, elem1._3))

    val workHoursCondition: Column = (count(
      when((col("hour_value") >= 9).and(col("hour_value") < 18), true)
    ).cast(DataTypes.DoubleType) / count(col("uid")).cast(DataTypes.DoubleType))
      .as("web_fraction_work_hours")

    val eveningHoursCondition: Column = (count(
      when((col("hour_value") >= 18).and(col("hour_value") < 23), true)
    ).cast(DataTypes.DoubleType) / count(col("uid")).cast(DataTypes.DoubleType))
      .as("web_fraction_evening_hours")

    val rawDF: DataFrame = logsDF
      .withColumn("day_value", Utils.getDayValue(col("timestamp")))
      .withColumn("hour_value", Utils.getHourValue(col("timestamp")))

    val dayAndHourDF: DataFrame = rawDF
      .groupBy(col("uid"))
      .agg(workHoursCondition, eveningHoursCondition)
      .select(col("uid"), col("web_fraction_work_hours"), col("web_fraction_evening_hours"))

    val dayDF: DataFrame =
      rawDF
        .groupBy(col("uid"))
        .pivot("day_value")
        .agg(dayColumns.head, dayColumns.drop(1): _*)

    val hourDF: DataFrame =
      rawDF
        .groupBy(col("uid"))
        .pivot("hour_value")
        .agg(hourColumns.head, hourColumns.drop(1): _*)

    dayDF
      .join(hourDF, Seq("uid"), "inner")
      .join(dayAndHourDF, Seq("uid"), "inner")

  }

}
