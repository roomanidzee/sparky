package com.romanidze.sparky.features.useroitemo

import org.apache.spark.sql.{DataFrame, SparkSession}

object MatrixDataLoader {

  def getMatrixDF(path: String)(implicit spark: SparkSession): DataFrame = {

    spark.read
      .parquet(path)
      .cache()

  }

}
