package com.romanidze.sparky.dashboard

import com.romanidze.sparky.dashboard.weblogs.{DataLoader, DataWriter}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class DashboardJob(implicit spark: SparkSession) {

  def start(): Unit = {

    val modelSavePath: String = spark.conf.get("spark.dashboard.model_save_path")
    val datasetPath: String = spark.conf.get("spark.dashboard.dataset_path")
    val esAddress: String = spark.conf.get("spark.dashboard.es_address")
    val esUsername: String = spark.conf.get("spark.dashboard.es_username")
    val esPassword: String = spark.conf.get("spark.dashboard.es_password")
    val esIndex: String = spark.conf.get("spark.dashboard.es_index")

    val model: PipelineModel = PipelineModel.load(modelSavePath)

    val dataLoader = new DataLoader()
    val dataWriter = new DataWriter()

    val rawDF: DataFrame = dataLoader.load(datasetPath)
    val mlDF: DataFrame = dataLoader.convertDF(rawDF)
    val resultDF: DataFrame = model.transform(mlDF)

    dataWriter.writeData(resultDF, esAddress, esIndex, esUsername, esPassword)

  }

}
