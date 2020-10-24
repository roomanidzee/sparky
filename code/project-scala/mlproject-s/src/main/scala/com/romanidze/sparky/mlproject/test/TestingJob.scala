package com.romanidze.sparky.mlproject.test

import com.romanidze.sparky.mlproject.test.weblogs.{DataLoader, DataWriter}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}

class TestingJob(implicit spark: SparkSession) {

  def start(): Unit = {

    val modelSavePath: String = spark.conf.get("spark.mlproject.model_save_path")
    val inputTopic: String = spark.conf.get("spark.mlproject.input_topic")
    val outputTopic: String = spark.conf.get("spark.mlproject.output_topic")
    val kafkaServers: String = spark.conf.get("spark.mlproject.kafka_servers")
    val checkpointPath: String = spark.conf.get("spark.mlproject.checkpoint_path")

    val model: PipelineModel = PipelineModel.load(modelSavePath)

    val dataLoader = new DataLoader()
    val dataWriter = new DataWriter()

    val rawDF: DataFrame = dataLoader.load(kafkaServers, inputTopic)
    val schemaDF: DataFrame = dataLoader.convertToSchemaDF(rawDF)
    val mlDF: DataFrame = dataLoader.getMLDF(schemaDF)
    val resultDF: DataFrame = model.transform(mlDF)

    dataWriter.writeData(resultDF, kafkaServers, outputTopic, checkpointPath)

  }

}
