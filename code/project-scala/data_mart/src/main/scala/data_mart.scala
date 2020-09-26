import com.romanidze.sparky.datamart.config.{ApplicationConfig, ConfigurationLoader}

object data_mart extends App {

  val appConfig: ApplicationConfig = ConfigurationLoader
    .load(args.head)
    .fold(e => sys.error(s"Failed to load configuration:\n${e.toList.mkString("\n")}"), identity)

}
