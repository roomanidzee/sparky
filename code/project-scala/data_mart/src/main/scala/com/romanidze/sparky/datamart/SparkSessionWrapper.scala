package com.romanidze.sparky.datamart

import com.romanidze.sparky.datamart.config.CassandraConfig
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  val config: CassandraConfig

  implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("DataMart App (Romanov Andrey)")
    .config("spark.cassandra.connection.host", config.host)
    .config("spark.cassandra.connection.port", config.port)
    .getOrCreate()

}
