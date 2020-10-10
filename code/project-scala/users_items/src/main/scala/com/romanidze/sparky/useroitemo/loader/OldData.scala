package com.romanidze.sparky.useroitemo.loader

import org.apache.spark.sql.SparkSession

object OldData {

  def load(inputDir: String, outputDir: String)(implicit spark: SparkSession): Unit = {
    println("old_data")
  }

}
