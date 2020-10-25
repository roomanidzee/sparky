package com.romanidze.sparky.dashboard.weblogs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class DataWriter {

  def writeData(
    sourceDF: DataFrame,
    address: String,
    targetIndex: String,
    username: String,
    password: String
  ): Unit = {

    val esConfig = Map(
      "es.nodes"               -> address,
      "es.batch.write.refresh" -> "false",
      "es.net.http.auth.user"  -> username,
      "es.net.http.auth.pass"  -> password
    )

    sourceDF
      .select(col("uid"), col("predicted_label").as("gender_age"), col("date"))
      .write
      .format("org.elasticsearch.spark.sql")
      .options(esConfig)
      .save(s"${targetIndex}/_doc")

  }

}
