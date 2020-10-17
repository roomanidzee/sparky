package com.romanidze.sparky.features.weblogs.processing

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TopSitesProcessing(implicit spark: SparkSession) {

  def getTopVisitedSites(rawDF: DataFrame): DataFrame = {

    rawDF
      .drop("uid")
      .groupBy(col("domain"))
      .agg(count(col("domain")).alias("domain_count"))
      .select(col("domain"), col("domain_count"))
      .orderBy(col("domain_count").desc)
      .limit(1000)
      .orderBy(col("domain").asc)
      .select(col("domain"))

  }

  def getDomainFeatures(logsDF: DataFrame, topSites: DataFrame): DataFrame = {

    val domainToUID = logsDF
      .filter(col("domain").isin(topSites.collect().map(_(0)).toList: _*))
      .orderBy(col("domain").asc)
      .groupBy(col("uid"), col("domain"))
      .count()
      .groupBy(col("uid"))
      .pivot("domain")
      .agg(sum("count"))
      .na
      .fill(0)

    domainToUID.select(
      col("uid"),
      array(
        domainToUID.columns
          .drop(1)
          .map(c => col(s"`$c`")): _*
      ).alias("domain_features")
    )

  }

  def getSitesDF(logsDF: DataFrame): DataFrame = {

    val topSitesDF: DataFrame = getTopVisitedSites(logsDF)

    getDomainFeatures(logsDF, topSitesDF)

  }

}
