package com.romanidze.sparky.features.weblogs.processing

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, TimestampType}

class TimeProcessing(implicit spark: SparkSession) {

  def getTimedDF(logsDF: DataFrame): (DataFrame, DataFrame) = {

    val dayRawDF: DataFrame =
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

    val dayDF: DataFrame = dayRawDF.select(
      col("uid"),
      col("mon"),
      col("tue"),
      col("wed"),
      col("thu"),
      col("fri"),
      col("sat"),
      col("sun")
    )

    val hourRawDF: DataFrame = logsDF
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

    val hourDF: DataFrame =
      hourRawDF
        .withColumn(
          "web_work_hours",
          col("9") + col("10") + col("11") + col("12") + col("13") +
          col("14") + col("15") + col("16") + col("17")
        )
        .withColumn(
          "web_evening_hours",
          col("18") + col("19") + col("20") + col("21")
          + col("22") + col("23")
        )
        .withColumn(
          "web_all_hours",
          col("web_work_hours") + col("web_evening_hours") + col("0") + col("1")
          + col("2") + col("3") + col("4") + col("5") + col("6")
          + col("7") + col("8")
        )
        .withColumn(
          "web_fraction_work_hours",
          col("web_work_hours").cast(DataTypes.DoubleType) / col("web_all_hours")
            .cast(DataTypes.DoubleType)
        )
        .withColumn(
          "web_fraction_evening_hours",
          col("web_evening_hours").cast(DataTypes.DoubleType) / col("web_all_hours")
            .cast(DataTypes.DoubleType)
        )
        .drop("web_work_hours", "web_evening_hours", "web_all_hours")

    val excludeHourColumns: Seq[String] =
      Seq("uid", "web_fraction_work_hours", "web_fraction_evening_hours")
    val renamedColumnHours: Seq[Column] =
      hourDF.columns
        .filter(elem => !excludeHourColumns.contains(elem))
        .map(name => col(name).as(s"web_hour_$name"))
        .toSeq

    val finalHourDF: DataFrame =
      hourDF.select((excludeHourColumns.map(elem => col(elem)) ++ renamedColumnHours): _*)

    val renamedDayColumns: Array[Column] =
      dayDF.columns.map(name => col(name).as(s"web_day_$name".toLowerCase))

    val finalDayDF: DataFrame =
      dayDF
        .select(renamedDayColumns: _*)
        .withColumnRenamed("web_day_uid", "uid")

    (finalDayDF, finalHourDF)

  }

}
