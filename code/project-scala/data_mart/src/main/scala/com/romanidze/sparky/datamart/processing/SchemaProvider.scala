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

}
