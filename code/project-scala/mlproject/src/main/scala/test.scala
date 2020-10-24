import com.romanidze.sparky.mlproject.test.TestingJob
import org.apache.spark.sql.SparkSession

object test extends App {

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("MLProject - Test (Romanov Andrey)")
      .getOrCreate()

  val job = new TestingJob()
  job.start()

  spark.streams.awaitAnyTermination()

}
