import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamWriter

object filter extends App{

  val spark: SparkSession = SparkSession.builder()
    .appName(" Logs Kafka2Spark (Romanov Andrey)")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  val topicName: String = spark.conf.get("spark.filter.topic_name")
  val offsetType: String = spark.conf.get("spark.filter.offset")
  val outputDirPrefix: String = spark.conf.get("spark.filter.output_dir_prefix")

  val kafkaParams: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> topicName,
    "startingOffsets" -> offsetType,
    "maxOffsetsPerTrigger" -> "5000"
  )

  val rawDF: DataFrame = spark.readStream
    .format("kafka")
    .options(kafkaParams)
    .load

  val schema = new StructType()
    .add("event_type", DataTypes.StringType, nullable = true)
    .add("category", DataTypes.StringType, nullable = true)
    .add("item_id", DataTypes.StringType, nullable = true)
    .add("item_price", DataTypes.LongType, nullable = true)
    .add("uid", DataTypes.StringType, nullable = true)
    .add("timestamp", DataTypes.LongType, nullable = true)

  val rawDataDF: DataFrame = rawDF.select(from_json(col("value").cast("string"), schema))
  val rawDataChangedDF: DataFrame =
    rawDataDF.withColumn("date", to_utc_timestamp(from_unixtime(col("timestamp"),"yyyyMMdd"),"UTC"))
             .withColumn("part_date", col("date"))

  val buyDataDF: DataFrame = rawDataChangedDF.filter(col("event_type") === "buy")
  val viewDataDF: DataFrame = rawDataChangedDF.filter(col("event_type") === "view")

  val checkpointBaseDir = "offsetsData"

  val buySink: DataStreamWriter[Row] = buyDataDF.writeStream
    .format("json")
    .partitionBy("part_date")
    .option("checkpointLocation", s"$checkpointBaseDir/buy")
    .option("path", s"$outputDirPrefix/buy")

  val viewSink: DataStreamWriter[Row] = viewDataDF.writeStream
    .format("json")
    .partitionBy("part_date")
    .option("checkpointLocation", s"$checkpointBaseDir/view")
    .option("path", s"$outputDirPrefix/view")

  buySink.start()
  viewSink.start()

  spark.streams.awaitAnyTermination()

}
