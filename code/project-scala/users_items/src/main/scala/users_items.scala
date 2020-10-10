import com.romanidze.sparky.useroitemo.loader.{NewData, OldData}
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object users_items extends App {

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Users and Items matrix (Romanov Andrey)")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  val conf: SparkConf = sc.getConf

  val log = LogManager.getRootLogger

  val inputDir: String = conf.get("spark.users_items.input_dir")
  val outputDir: String = conf.get("spark.users_items.output_dir")
  val modeType: String = conf.get("spark.users_items.update", "1")

  log.info(s"INPUT DIR: $inputDir")
  log.info(s"OUTPUT DIR: $outputDir")
  log.info(s"MODE TYPE: $modeType")

  modeType match {
    case "0" => NewData.load(inputDir, outputDir)
    case "1" => OldData.load(inputDir, outputDir)
  }

  spark.stop()

}
