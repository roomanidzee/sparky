package com.romanidze.sparky.useroitemo.loader

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object NewData {

  def load(
    inputDir: String,
    outputDir: String
  )(implicit spark: SparkSession, sc: SparkContext): Unit = {
    println("new_data")
  }

}
