package com.romanidze.sparky.datamart.processing.clients

import com.romanidze.sparky.datamart.config.CassandraConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

class ClientInfoLoader(config: CassandraConfig)(implicit spark: SparkSession) {

  def getClientsDataFrame: DataFrame = {

    spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", config.keyspace)
      .option("table", config.table)
      .load()

  }

}
