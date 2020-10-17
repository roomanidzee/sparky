import com.romanidze.sparky.features.FeatureMatrixJob
import org.apache.spark.sql.SparkSession

object features extends App {

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Feature Matrix (Romanov Andrey)")
      .getOrCreate()

  val job = new FeatureMatrixJob()
  job.start()
  spark.stop()

}
