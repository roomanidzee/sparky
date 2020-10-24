package com.romanidze.sparky.mlproject.test.weblogs

import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}

object SchemaProvider {

  val testDatasetSchema: StructType = StructType(
    List(
      StructField("uid", DataTypes.StringType),
      StructField(
        "visits",
        ArrayType(
          StructType(
            List(
              StructField("url", DataTypes.StringType),
              StructField("timestamp", DataTypes.LongType)
            )
          )
        )
      )
    )
  )

}
