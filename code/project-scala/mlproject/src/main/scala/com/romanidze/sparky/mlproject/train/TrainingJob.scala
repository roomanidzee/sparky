package com.romanidze.sparky.mlproject.train

import com.romanidze.sparky.mlproject.train.weblogs.{DataLoader, PipelinePreparing}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TrainingJob(implicit spark: SparkSession) {

  def start(): Unit = {

    val datasetPath: String = spark.conf.get("spark.mlproject.dataset_path")
    val modelSavePath: String = spark.conf.get("spark.mlproject.model_save_path")

    val dataLoader = new DataLoader()
    val logsRawDF: DataFrame = dataLoader.load(datasetPath)
    val logsMLDF: DataFrame = dataLoader.convertDF(logsRawDF)

    val pipeline: Pipeline = PipelinePreparing.getPipeline(logsMLDF)

    val model: PipelineModel = pipeline.fit(logsMLDF)

    model.write
      .overwrite()
      .save(modelSavePath)

  }

}
