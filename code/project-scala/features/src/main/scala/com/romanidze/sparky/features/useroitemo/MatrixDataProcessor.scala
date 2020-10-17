package com.romanidze.sparky.features.useroitemo

import org.apache.spark.sql.{DataFrame, SparkSession}

object MatrixDataProcessor {

  def getJoinedDF(matrixDF: DataFrame, domainFeaturesDF: DataFrame, logsTimeDF: DataFrame)(
    implicit spark: SparkSession
  ): DataFrame = {

    matrixDF
      .join(domainFeaturesDF, Seq("uid"), "left")
      .join(logsTimeDF, Seq("uid"), "left")

  }

}
