package com.romanidze.sparky.relevantsites

import com.romanidze.sparky.relevantsites.classes.Record
import com.romanidze.sparky.relevantsites.processing.DataLoader
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

object RelevantSitesApp {

  def main(args:Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .appName("Relevant Sites App (Romanov Andrey)")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val rawAutos: DataFrame = spark.read.json("/labs/laba02/autousers.json")
    val autoDF: DataFrame = rawAutos.select(explode(rawAutos("autousers")))
                                    .toDF("uid")

    autoDF.show(15)

    autoDF.createOrReplaceTempView("auto_df")

    val rawData: RDD[Record] = sc.textFile("/labs/laba02/logs")
                                 .map(elem => DataLoader.processRecord(elem))
                                 .filter(elem => !elem.uid.equals(0L) && !elem.url.equals("-"))

    val recordDFData: DataFrame = rawData.toDF()

    recordDFData.show(15)

    recordDFData.createOrReplaceTempView("record_data")

    val recordBinaryDF: DataFrame = spark.sql(
      """
        |SELECT url, autos.uid
        |FROM record_data records
        |LEFT JOIN auto_df autos ON (records.uid=autos.uid)
        |""".stripMargin
    )
      .withColumn("auto_flag", when(col("autos.uid").isNull, 0).otherwise(1))
      .drop(col("autos.uid"))

    recordBinaryDF.show(15)



    val calcRelevance: UserDefinedFunction = udf { (urlAutoCount: Long, urlCount: Long, autoCount: Long, domainCount: Long) => {

        val castedUrlAutoCount: Double = urlAutoCount.toDouble
        val castedUrlCount: Double = urlCount.toDouble
        val castedAutoCount: Double = autoCount.toDouble
        val castedDomainCount: Double = domainCount.toDouble

        val numerator: Double = ( (castedUrlAutoCount / castedDomainCount) * (castedUrlAutoCount / castedDomainCount) )

        val denominator: Double = ( (castedUrlCount / castedDomainCount) * (castedAutoCount / castedDomainCount))

        numerator / denominator

      }

    }

    val urlAutoDF: DataFrame = recordBinaryDF.groupBy(col("url"))
                                             .agg(
                                               count(
                                                 when(
                                                   $"auto_flag" === 1, true
                                                 )
                                               ).alias("url_auto_count")
                                             )

    urlAutoDF.show(15)

    val urlCount: DataFrame = recordBinaryDF.groupBy(col("url"))
                                            .agg(
                                              count(col("url")).alias("url_count")
                                            )

    urlCount.show(15)

    val autoCount: DataFrame = recordBinaryDF.agg(
      count(
        when(
          $"auto_flag" === 1, true
        )
      ).alias("domain_count")
    )
    autoCount.show()


    val calcDF: DataFrame = urlAutoDF.join(urlCount, Seq("url"), "inner")
                                     .filter($"url_auto_count" =!= 0)

    calcDF.show(15)

    calcDF.createOrReplaceTempView("calc_df")

    val joinedDF: DataFrame = spark.sql(
      """
        |SELECT calc_data.url AS url, url_auto_count, url_count
        |FROM calc_df calc_data
        |LEFT JOIN record_data records ON (calc_data.url=records.url)
        |""".stripMargin
    ).na.drop("all")
        .distinct()
        .withColumn("autoCount", lit(autoCount.collect()(0).getLong(0)))
        .withColumn("domainCount", lit(recordBinaryDF.count()))

    joinedDF.show(15)

    val resultDF: DataFrame = joinedDF.select(
      col("url"),
      calcRelevance(
        col("url_auto_count"),
        col("url_count"),
        col("autoCount"),
        col("domainCount")
      ).alias("relevance")
    ).orderBy(desc("relevance"), col("url"))

    resultDF.show(15)

    resultDF.limit(200)
            .repartition(1)
            .rdd
            .map(_.mkString("\t"))
            .saveAsTextFile("domains")

    spark.stop()


  }

}
