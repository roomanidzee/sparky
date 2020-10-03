import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object agg extends App {

  val sourceTopic: String = "andrey_romanov"
  val sinkTopic: String = "andrey_romanov_lab04b_out"
  val checkpointDir: String = "checkpointData"

  val spark: SparkSession = SparkSession
    .builder()
    .appName(" Data Aggregation with Spark Streaming (Romanov Andrey)")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  val schema = new StructType()
    .add("event_type", DataTypes.StringType, nullable = true)
    .add("category", DataTypes.StringType, nullable = true)
    .add("item_id", DataTypes.StringType, nullable = true)
    .add("item_price", DataTypes.LongType, nullable = true)
    .add("uid", DataTypes.StringType, nullable = true)
    .add("timestamp", DataTypes.LongType, nullable = true)

  val rawDF: DataFrame = spark.readStream
    .format("kafka")
    .options(
      Map(
        "kafka.bootstrap.servers" -> "spark-master-1:6667",
        "subscribe"               -> sourceTopic,
        "maxOffsetsPerTrigger"    -> "5000"
      )
    )
    .load

  val rawStringDF: DataFrame = rawDF.selectExpr("CAST(value AS STRING)")

  val rawDataDF: DataFrame = rawStringDF
    .select(from_json(col("value"), schema).as("data"))
    .select("data.*")

  val rawDataChangedDF: DataFrame =
    rawDataDF.withColumn("timestamp_converted", (col("timestamp") / 1000).cast(TimestampType))

  val aggregatedDF: DataFrame = rawDataChangedDF
    .withWatermark("timestamp_converted", "1 hour")
    .groupBy(window(col("timestamp_converted"), "1 hour", "1 hour"))
    .agg(
      sum(when(col("event_type") === "buy", col("item_price")).as("item_price_agg")).as("revenue"),
      count(when(col("uid").isNotNull, true)).as("visitors"),
      count(when(col("event_type") === "buy", true)).as("purchases")
    )

  val aggregatedChangedDF: DataFrame = aggregatedDF
    .withColumn(
      "aov",
      col("revenue").cast(DataTypes.DoubleType) / col("purchases").cast(DataTypes.DoubleType)
    )
    .select(
      unix_timestamp(col("window.start")).as("start_ts"),
      unix_timestamp(col("window.end")).as("end_ts"),
      col("revenue"),
      col("visitors"),
      col("purchases"),
      col("aov")
    )

  aggregatedChangedDF.toJSON.writeStream
    .format("kafka")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .options(
      Map(
        "kafka.bootstrap.servers" -> "10.0.0.5:6667",
        "topic"                   -> sinkTopic,
        "checkpointLocation"      -> s"chk/$checkpointDir"
      )
    )
    .outputMode(OutputMode.Update())
    .start()

  spark.streams.awaitAnyTermination()

}
