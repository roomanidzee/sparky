import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType, TimestampType}
import org.apache.spark.sql.functions._

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
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", sourceTopic)
    .option("maxOffsetsPerTrigger", "5000")
    .load

  val rawStringDF: DataFrame = rawDF.selectExpr("CAST(value AS STRING)")

  val rawDataDF: DataFrame = rawStringDF
    .select(from_json(col("value"), schema).as("data"))
    .select("data.*")

  val rawDataChangedDF: DataFrame =
    rawDataDF.withColumn("timestamp_converted", (col("timestamp") / 1000).cast(TimestampType))

}
