package com.romanidze.sparky.relevantsites

import com.romanidze.sparky.relevantsites.classes.Record
import com.romanidze.sparky.relevantsites.processing.DataLoader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object RelevantSitesApp {

  def main(args:Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Relevant Sites App (Romanov Andrey)")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val uidCond: Record => Boolean = (elem: Record) => elem.uid.equals("-") || elem.uid.isEmpty
    val urlCond: Record => Boolean = (elem: Record) => elem.url.equals("-")

    import spark.implicits._

    val rawData: RDD[Record] = sc.textFile("/labs/laba02/logs")
                                 .map(elem => DataLoader.processRecord(elem))
                                 .filter(elem => !( uidCond.apply(elem) || urlCond.apply(elem) ) )

    val recordDFData: DataFrame = rawData.toDF()

    val auto = spark.read.json("/labs/laba02/autousers.json")

  }

}
