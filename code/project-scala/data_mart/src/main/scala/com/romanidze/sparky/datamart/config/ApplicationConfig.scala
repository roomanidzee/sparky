package com.romanidze.sparky.datamart.config

//TODO: refined types
case class ApplicationConfig(
  psql: PostgreSQLConfig,
  cassandra: CassandraConfig,
  elasticsearch: ElasticSearchConfig
)
