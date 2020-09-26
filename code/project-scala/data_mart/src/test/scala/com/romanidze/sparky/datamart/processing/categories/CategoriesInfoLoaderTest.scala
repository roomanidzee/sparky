package com.romanidze.sparky.datamart.processing.categories

import com.holdenkarau.spark.testing.SharedSparkContext
import com.romanidze.sparky.datamart.config.{DataInfo, PostgreSQLConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CategoriesInfoLoaderTest extends AnyFlatSpec with Matchers with SharedSparkContext {

  it should "correctly load information for categories" in {

    implicit val spark: SparkSession = SparkSession.builder.config(sc.getConf).getOrCreate()

    val psqlConfig = PostgreSQLConfig(
      "localhost",
      "5432",
      "default_user",
      "default_pass",
      DataInfo("default_db", "domain_cats"),
      DataInfo("", "")
    )

    val categoryInfoLoader = new CategoryInfoLoader(psqlConfig)

    val categoriesDF: DataFrame = categoryInfoLoader.getCategoriesDataFrame

    categoriesDF.printSchema()
    categoriesDF.show(1, 200, vertical = true)
    categoriesDF.count() > 0 shouldBe true

    spark.stop()

  }

}
