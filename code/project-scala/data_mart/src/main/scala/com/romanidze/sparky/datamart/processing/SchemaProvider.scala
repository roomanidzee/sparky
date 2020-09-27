package com.romanidze.sparky.datamart.processing

import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}

object SchemaProvider {

  def getWebLogsSchema: StructType = {

    StructType(
      Seq(
        StructField("uid", DataTypes.StringType, nullable = true),
        StructField(
          "visits",
          ArrayType(
            StructType(
              Seq(
                StructField("timestamp", DataTypes.LongType),
                StructField("url", DataTypes.StringType)
              )
            )
          ),
          nullable = true
        )
      )
    )

  }

  def getWebLogsAggregatedSchema: StructType = {

    StructType(
      Seq(
        StructField("uid", DataTypes.StringType),
        StructField("url", DataTypes.StringType),
        StructField("url_count", DataTypes.LongType, nullable = false)
      )
    )

  }

  def getCategoriesSchema: StructType = {

    StructType(
      Seq(
        StructField("domain", DataTypes.StringType, nullable = true),
        StructField("category", DataTypes.StringType, nullable = true)
      )
    )

  }

  def getCategoriesAggregatedSchema: StructType = {

    StructType(
      Seq(
        StructField("uid", DataTypes.StringType, nullable = true),
        StructField("url_count", DataTypes.LongType, nullable = false),
        StructField("category", DataTypes.StringType, nullable = true),
        StructField("web_category", DataTypes.StringType, nullable = true)
      )
    )

  }

  def getShopInfoSchema: StructType = {

    StructType(
      Seq(
        StructField("category", DataTypes.StringType, nullable = true),
        StructField("event_type", DataTypes.StringType, nullable = true),
        StructField("item_id", DataTypes.StringType, nullable = true),
        StructField("item_price", DataTypes.LongType, nullable = true),
        StructField("timestamp", DataTypes.LongType, nullable = true),
        StructField("uid", DataTypes.StringType, nullable = true)
      )
    )

  }

  def getShopAggregatedSchema: StructType = {

    StructType(
      Seq(
        StructField("uid", DataTypes.StringType, nullable = true),
        StructField("action_count", DataTypes.LongType, nullable = false),
        StructField("category", DataTypes.StringType, nullable = true),
        StructField("shop_category", DataTypes.StringType, nullable = true)
      )
    )

  }

}
