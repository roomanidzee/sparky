package com.romanidze.sparky.features.weblogs.processing

import com.romanidze.sparky.features.weblogs.Utils
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, TimestampType}

class TimeProcessing(implicit spark: SparkSession) {

  def getTimedDF(logsDF: DataFrame): DataFrame = {

    val workHoursCondition: Column = (count(
      when((col("hour_value") >= 9).and(col("hour_value") < 18), true)
    ).cast(DataTypes.DoubleType) / count(col("uid")).cast(DataTypes.DoubleType))
      .as("web_fraction_work_hours")

    val eveningHoursCondition: Column = (count(
      when((col("hour_value") >= 18).and(col("hour_value") < 23), true)
    ).cast(DataTypes.DoubleType) / count(col("uid")).cast(DataTypes.DoubleType))
      .as("web_fraction_evening_hours")

    val hourRange: Seq[Column] = (0 to 23).map(elem => col(s"$elem").as(s"web_hour_$elem"))
    val dayRange: Seq[Column] = (1 to 7).map(elem => col(s"$elem").as(s"web_day_$elem"))

    val hourDF: DataFrame =
      logsDF
        .select(
          col("uid"),
          hour(to_utc_timestamp((col("timestamp") / 1000).cast(TimestampType), "Europe/Moscow"))
            .as("hour")
        )
        .groupBy(col("uid"))
        .pivot(col("hour"))
        .count()
        .na
        .fill(0)
        .select((Seq(col("uid")) ++ hourRange): _*)

    val dayDF: DataFrame =
      logsDF
        .select(
          col("uid"),
          date_format(
            to_utc_timestamp((col("timestamp") / 1000).cast(TimestampType), "Europe/Moscow"),
            "EEE"
          ).as("day")
        )
        .groupBy(col("uid"))
        .pivot(col("day"))
        .count()
        .na
        .fill(0)
        .select((Seq(col("uid")) ++ dayRange): _*)

    val rawDF: DataFrame = logsDF
      .withColumn("day_value", Utils.getDayValue(col("timestamp")))
      .withColumn("hour_value", Utils.getHourValue(col("timestamp")))

    val dayAndHourDF: DataFrame = rawDF
      .groupBy(col("uid"))
      .agg(workHoursCondition, eveningHoursCondition)
      .select(col("uid"), col("web_fraction_work_hours"), col("web_fraction_evening_hours"))
      .dropDuplicates(Seq("uid"))

    dayDF
      .join(hourDF, Seq("uid"), "inner")
      .join(dayAndHourDF, Seq("uid"), "inner")

  }

}
