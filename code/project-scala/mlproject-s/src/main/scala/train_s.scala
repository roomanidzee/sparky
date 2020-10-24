import com.romanidze.sparky.mlproject.train.TrainingJob
import org.apache.spark.sql.SparkSession

object train_s extends App {

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("MLProject(custom version) - Train (Romanov Andrey)")
      .getOrCreate()

  val job = new TrainingJob()
  job.start()

  spark.stop()

}
