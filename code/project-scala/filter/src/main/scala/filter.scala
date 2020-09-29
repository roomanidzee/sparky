import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.DataTypes

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

  implicit val myObjEncoder: Encoder[String] = org.apache.spark.sql.Encoders.STRING
  val jsonString = rawDF.select(col("value").cast(DataTypes.StringType)).as[String]

  val rawDataDF: DataFrame = spark.read.json(jsonString)
  val rawDataChangedDF: DataFrame =
    rawDataDF.withColumn("date", to_utc_timestamp(from_unixtime(col("timestamp"),"yyyyMMdd"),"UTC"))
      .withColumn("part_date", col("date"))

  rawDataChangedDF.createOrReplaceTempView("raw_data_df")
  val viewDataDF: DataFrame = rawDataChangedDF.filter(col("event_type") === "view")

  val checkpointBaseDir = "offsetsData"

  val buySink: DataStreamWriter[Row] =
    spark.sql("SELECT * FROM raw_data_df WHERE event_type == 'buy'")
      .writeStream
      .format("json")
      .partitionBy("part_date")
      .option("checkpointLocation", s"$checkpointBaseDir/buy")
      .option("path", s"$outputDirPrefix/buy")

  val viewSink: DataStreamWriter[Row] =
    spark.sql("SELECT * FROM raw_data_df WHERE event_type == 'view'")
      .writeStream
      .format("json")
      .partitionBy("part_date")
      .option("checkpointLocation", s"$checkpointBaseDir/view")
      .option("path", s"$outputDirPrefix/view")

  buySink.start()
  viewSink.start()

  spark.streams.awaitAnyTermination()

}
