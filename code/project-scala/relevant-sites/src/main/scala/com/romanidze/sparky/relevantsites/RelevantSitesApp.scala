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

    val uidCond: Record => Boolean = (elem: Record) => elem.uid.equals("-") || elem.uid.isEmpty
    val urlCond: Record => Boolean = (elem: Record) => elem.url.equals("-") || elem.url.isEmpty

    import spark.implicits._

    val rawAutos: DataFrame = spark.read.json("/labs/laba02/autousers.json")
    val autoDF: DataFrame = rawAutos.select(explode(rawAutos("autousers")))
                                    .toDF("uid")

    autoDF.createOrReplaceTempView("auto_df")

    val rawData: RDD[Record] = sc.textFile("/labs/laba02/logs")
                                 .map(elem => DataLoader.processRecord(elem))
                                 .filter(elem => !( uidCond.apply(elem) || urlCond.apply(elem) ) )

    val recordDFData: DataFrame = rawData.toDF()

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

    val dfSize: Long = recordBinaryDF.count()

    val calcRelevance: UserDefinedFunction = udf { (urlAutoCount: Long, urlCount: Long, autoCount: Long, domainCount: Long) => {

        val numerator: Double = ( (urlAutoCount / domainCount) * (urlAutoCount / domainCount) ).toDouble

        val denominator: Double = ( (urlCount / domainCount) * (autoCount / domainCount)).toDouble

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

    val urlCount: DataFrame = recordBinaryDF.groupBy(col("url"))
                                            .agg(
                                              count(col("url")).alias("url_count")
                                            )

    val autoCount: Long = recordBinaryDF.agg(
      count(
        when(
          $"auto_flag" === 1, true
        )
      ).alias("domain_count")
    ).collect()(0).getLong(0)

    val calcDF: DataFrame = urlAutoDF.join(urlCount, Seq("url"), "inner")

    val joinedDF: DataFrame = recordBinaryDF.join(calcDF, Seq("url"), "inner")

    val resultDF: DataFrame = joinedDF.select(
      col("url"),
      round(
        calcRelevance(
          col("url_auto_count"),
          col("url_count"),
          lit(autoCount),
          lit(dfSize)
        ), 20
      ).alias("relevance")
    ).orderBy(desc("relevance"), col("url"))

    resultDF.repartition(1)
            .rdd
            .map(_.mkString("\t"))
            .saveAsTextFile("file:///home/andrey.romanov/laba02_domains.txt")

    spark.stop()


  }

}
