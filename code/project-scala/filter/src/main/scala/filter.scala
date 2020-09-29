import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object filter extends App{

  val spark:SparkSession = SparkSession.builder()
    .appName(" Logs Kafka2Spark (Romanov Andrey)")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  val topicName: String = spark.conf.get("spark.filter.topic_name")
  val offsetType: String = spark.conf.get("spark.filter.offset")
  val outputDirPrefix: String = spark.conf.get("spark.filter.output_dir_prefix")

}
