package com.romanidze.sparky.features

import com.romanidze.sparky.features.useroitemo.{MatrixDataLoader, MatrixDataProcessor}
import com.romanidze.sparky.features.weblogs.LogsDataLoader
import com.romanidze.sparky.features.weblogs.processing.{TimeProcessing, TopSitesProcessing}
import org.apache.spark.sql.{DataFrame, SparkSession}

class FeatureMatrixJob(implicit val spark: SparkSession) {

  def start(): Unit = {

    val matrixOriginDF: DataFrame =
      MatrixDataLoader.getMatrixDF("/user/andrey.romanov/users-items/*")
    val logsOriginDF: DataFrame = LogsDataLoader.getLogsDF

    val logsTimeProcessor = new TimeProcessing()
    val logsSitesProcessor = new TopSitesProcessing()

    val logsTimedDF: DataFrame = logsTimeProcessor.getTimedDF(logsOriginDF)
    val logsTopsSitesDF: DataFrame = logsSitesProcessor.getSitesDF(logsOriginDF)

    val finalDF = MatrixDataProcessor.getJoinedDF(matrixOriginDF, logsTopsSitesDF, logsTimedDF)

    finalDF.write.parquet("/user/andrey.romanov/features")

  }

}
