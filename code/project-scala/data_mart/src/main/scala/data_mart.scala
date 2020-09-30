import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object data_mart extends App {

  implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("DataMart App (Romanov Andrey)")
    .config("spark.cassandra.connection.host", cassandraConfig("host"))
    .config("spark.cassandra.connection.port", cassandraConfig("port"))
    .config("spark.cassandra.output.consistency.level", "ANY")
    .config("spark.cassandra.input.consistency.level", "ONE")
    .getOrCreate()

  val postgresSrcConfig = Map(
    "url"      -> "jdbc:postgresql://10.0.0.5:5432/labdata",
    "user"     -> "andrey_romanov",
    "password" -> "3ytIi9s2"
  )

  val postgresSinkConfig = Map(
    "url"      -> "jdbc:postgresql://10.0.0.5:5432/andrey_romanov",
    "user"     -> "andrey_romanov",
    "password" -> "3ytIi9s2"
  )

  val cassandraConfig = Map("host" -> "10.0.0.5", "port" -> "9042")

  val esConfig = Map(
    "address"                -> "10.0.0.5:9200",
    "es.batch.write.refresh" -> "false",
  )

  val categoriesDF: DataFrame = spark.read
    .format("jdbc")
    .option("url", postgresSrcConfig("url"))
    .option("user", postgresSrcConfig("user"))
    .option("password", postgresSrcConfig("password"))
    .option("driver", "org.postgresql.Driver")
    .option("query", "select * from domain_cats")
    .load
    .withColumn("web_category", concat(lit("web_"), col("category")))
    .cache()

  val clientsDF: DataFrame = spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "clients", "keyspace" -> "labdata"))
    .load()
    .cache()

  val shopDF: DataFrame = spark.read
    .format("org.elasticsearch.spark.sql")
    .options(esConfig)
    .load("visits*")
    .withColumn(
      "shop_category",
      concat(lit("shop_"), lower(regexp_replace(col("category"), "[\\s-]+", "_")))
    )
    .select(col("shop_category"), col("uid"))
    .where("uid is not null")
    .cache()

  val logsDF: DataFrame = spark.read
    .json("/labs/laba03/weblogs.json")
    .select(col("uid"), explode(col("visits")).as('visit))
    .select(col("uid"), lower(col("visit.url").as("url")))
    .drop("visit")
    .withColumn("domain", regexp_replace(callUDF("parse_url", col("url"), lit("HOST")), "www.", ""))
    .drop("url")
    .cache()

  val logsAggregated: DataFrame = logsDF
    .join(categoriesDF, Seq("domain"), "left")
    .groupBy(col("uid"))
    .pivot("category")
    .agg(count(col("uid")))
    .drop("null")
    .drop("category")
    .orderBy(col("uid"))

  val shopAggregated: DataFrame = shopDF
    .groupBy(col("uid"))
    .pivot("category")
    .agg(count(col("uid")))
    .drop("category")
    .filter(col("uid").isNotNull)
    .orderBy(col("uid"))

  val clientsAggregated: DataFrame = clientsDF
    .withColumn(
      "age_cat",
      when(col("age") >= 18 && col("age") <= 24, "18-24")
        .when(col("age") >= 25 && col("age") <= 34, "25-34")
        .when(col("age") >= 35 && col("age") <= 44, "35-44")
        .when(col("age") >= 45 && col("age") <= 54, "45-54")
        .when(col("age") >= 55, ">=55")
        .otherwise(0)
    )
    .select(col("uid"), col("gender"), col("age_cat"))

  val resultDF: DataFrame = clientsAggregated
    .join(shopAggregated, Seq("uid"), "left")
    .join(logsAggregated, Seq("uid"), "left")

  resultDF.write
    .format("jdbc")
    .option("url", postgresSinkConfig("url"))
    .option("dbtable", "clients")
    .option("user", postgresSinkConfig("user"))
    .option("password", postgresSinkConfig("password"))
    .option("driver", "org.postgresql.Driver")
    .save()

  spark.stop()

}
