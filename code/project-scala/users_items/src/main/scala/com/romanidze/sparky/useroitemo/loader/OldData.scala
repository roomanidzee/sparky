package com.romanidze.sparky.useroitemo.loader

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object OldData {

  def load(
    inputDir: String,
    outputDir: String
  )(implicit spark: SparkSession, sc: SparkContext): Unit = {
    println("old_data")
  }

}
