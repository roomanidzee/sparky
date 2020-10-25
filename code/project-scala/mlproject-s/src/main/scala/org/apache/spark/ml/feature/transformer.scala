package org.apache.spark.ml.feature

import com.romanidze.sparky.mlproject.train.weblogs.SchemaProvider
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

class Url2DomainTransformer(override val uid: String)
    extends Transformer
    with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("org.apache.spark.ml.feature.Url2DomainTransformer"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    val logsRawDF: DataFrame =
      dataset.select(col("uid"), col("gender_age"), lower(col("visit.url")).as("url"))

    val logsDF: DataFrame = logsRawDF
      .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .select(col("uid"), col("gender_age"), col("domain"))
      .na
      .drop("any")

    logsDF
      .groupBy(col("uid"), col("gender_age"))
      .agg(collect_list("domain").as("domains"))
  }

  override def copy(extra: ParamMap): Url2DomainTransformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = SchemaProvider.url2DomainSchema

}

object Url2DomainTransformer extends DefaultParamsReadable[Url2DomainTransformer] {
  override def load(path: String): Url2DomainTransformer = super.load(path)
}
