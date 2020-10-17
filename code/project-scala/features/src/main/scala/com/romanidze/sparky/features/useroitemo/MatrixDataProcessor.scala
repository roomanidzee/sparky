package com.romanidze.sparky.features.useroitemo

import org.apache.spark.sql.{DataFrame, SparkSession}

object MatrixDataProcessor {

  def getJoinedDF(
    matrixDF: DataFrame,
    domainFeaturesDF: DataFrame,
    logsTimeDF: (DataFrame, DataFrame)
  )(implicit spark: SparkSession): DataFrame = {

    val dayDF: DataFrame = logsTimeDF._1
    val hourDF: DataFrame = logsTimeDF._2

    hourDF
      .join(dayDF, Seq("uid"), "left")
      .join(domainFeaturesDF, Seq("uid"), "left")
      .join(matrixDF, Seq("uid"), "left")
      .na
      .fill(0)

  }

}
