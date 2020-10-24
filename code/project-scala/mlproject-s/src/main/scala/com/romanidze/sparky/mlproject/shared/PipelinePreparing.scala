package com.romanidze.sparky.mlproject.shared

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{SklearnEstimator, Url2DomainTransformer}

object PipelinePreparing {

  def getPipeline: Pipeline = {

    val transformer = new Url2DomainTransformer()
    val estimator = new SklearnEstimator()

    val pipeline = new Pipeline()
      .setStages(Array(transformer, estimator))

    pipeline

  }

}
