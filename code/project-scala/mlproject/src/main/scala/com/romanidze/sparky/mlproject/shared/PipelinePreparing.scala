package com.romanidze.sparky.mlproject.shared

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.sql.DataFrame

object PipelinePreparing {

  def getPipeline(featureDF: DataFrame): Pipeline = {

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val indexConverter = new IndexToString()
      .setInputCol(lr.getPredictionCol)
      .setOutputCol("predicted_label")
      .setLabels(indexer.fit(featureDF).labels)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, indexConverter))

    pipeline

  }

}
