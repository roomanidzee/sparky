import com.romanidze.sparky.datamart.SparkSessionWrapper
import com.romanidze.sparky.datamart.config.{ApplicationConfig, CassandraConfig, ConfigurationLoader}
import com.romanidze.sparky.datamart.jobs.DataMartJob

object data_mart extends App with SparkSessionWrapper {

  val appConfig: ApplicationConfig = ConfigurationLoader.load
    .fold(e => sys.error(s"Failed to load configuration:\n${e.toList.mkString("\n")}"), identity)

  override val config: CassandraConfig = appConfig.cassandra

  val job = new DataMartJob(appConfig)
  job.start()

  spark.stop()

}
