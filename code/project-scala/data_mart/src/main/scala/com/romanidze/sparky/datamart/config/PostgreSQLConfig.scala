package com.romanidze.sparky.datamart.config

case class DataInfo(database: String, table: String)

case class PostgreSQLConfig(
  host: String,
  port: String,
  user: String,
  password: String,
  source: DataInfo,
  sink: DataInfo
)
