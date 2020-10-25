import com.romanidze.sparky.dashboard.DashboardJob
import org.apache.spark.sql.SparkSession

object dashboard extends App {

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Dashboard Job (Romanov Andrey)")
      .getOrCreate()

  val job = new DashboardJob()
  job.start()

  spark.stop()

}
