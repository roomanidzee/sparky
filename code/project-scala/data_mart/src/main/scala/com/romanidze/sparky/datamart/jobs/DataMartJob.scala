package com.romanidze.sparky.datamart.jobs

import com.romanidze.sparky.datamart.config.ApplicationConfig
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class DataMartJob(config: ApplicationConfig)(implicit spark: SparkSession) extends CommonJob {

  import spark.implicits._

  implicit val sc: SparkContext = spark.sparkContext

  def start(): Unit = {
    println("hallo")
  }

}
