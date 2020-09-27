package com.romanidze.sparky.datamart.jobs

import com.romanidze.sparky.datamart.config.{ApplicationConfig, PostgreSQLConfig}
import com.romanidze.sparky.datamart.processing.categories.CategoryInfoLoader
import com.romanidze.sparky.datamart.processing.clients.ClientInfoLoader
import com.romanidze.sparky.datamart.processing.shop.ShopInfoLoader
import com.romanidze.sparky.datamart.processing.weblogs.WebLogsLoader
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataMartJob(config: ApplicationConfig)(implicit spark: SparkSession) extends CommonJob {

  import spark.implicits._

  implicit val sc: SparkContext = spark.sparkContext

  def start(): Unit = {

    val categoryInfoLoader = new CategoryInfoLoader(config.psql)
    val clientInfoLoader = new ClientInfoLoader(config.cassandra)
    val shopInfoLoader = new ShopInfoLoader(config.elasticsearch)
    val webLogsLoader = new WebLogsLoader()

    val webLogsDF: DataFrame = webLogsLoader.getWebLogsDataFrame("/labs/laba03/weblogs.parquet")
    val categoriesDF: DataFrame = categoryInfoLoader.getCategoriesDataFrame
    val clientDF: DataFrame = clientInfoLoader.getClientsDataFrame
    val shopDF: DataFrame = shopInfoLoader.getShopInfoDataFrame

    val processedLogsDF: DataFrame = webLogsLoader.processLogsDataFrame(webLogsDF)

    val categoriesJoinedDF: DataFrame =
      categoryInfoLoader.getJoinedDF(processedLogsDF, categoriesDF)
    val shopProcessedDF: DataFrame = shopInfoLoader.getAggregatedDataFrame(shopDF)

    val clientJoinedDF: DataFrame =
      clientInfoLoader.getJoinedDataFrame(clientDF, categoriesJoinedDF, shopProcessedDF)

    val pivotDF: DataFrame = clientInfoLoader.getPivotDataFrame(clientJoinedDF)

    val dbSinkConfig: PostgreSQLConfig = config.psql

    pivotDF.write
      .format("jdbc")
      .option(
        "url",
        s"jdbc:postgresql://${dbSinkConfig.host}:${dbSinkConfig.port}/${dbSinkConfig.sink.database}"
      )
      .option("dbtable", dbSinkConfig.sink.table)
      .option("user", dbSinkConfig.user)
      .option("password", dbSinkConfig.password)
      .option("driver", "org.postgresql.Driver")
      .mode("overwrite")
      .save()

  }

}
