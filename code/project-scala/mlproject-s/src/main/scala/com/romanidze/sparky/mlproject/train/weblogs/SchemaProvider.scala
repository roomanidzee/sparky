package com.romanidze.sparky.mlproject.train.weblogs

import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}

object SchemaProvider {

  val url2DomainSchema: StructType = StructType(
    List(
      StructField("uid", DataTypes.StringType),
      StructField("gender_age", DataTypes.StringType),
      StructField("domains", ArrayType(DataTypes.StringType))
    )
  )

}
