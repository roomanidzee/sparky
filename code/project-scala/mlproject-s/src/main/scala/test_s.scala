import com.romanidze.sparky.mlproject.test.TestingJob
import org.apache.spark.sql.SparkSession

object test_s extends App {

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("MLProject(custom version) - Test (Romanov Andrey)")
      .getOrCreate()

  val job = new TestingJob()
  job.start()

  spark.streams.awaitAnyTermination()

}
