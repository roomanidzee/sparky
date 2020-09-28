package com.romanidze.sparky.datamart.config

import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import pureconfig.generic.auto._

object ConfigurationLoader {

  def load(path: String): Either[ConfigReaderFailures, ApplicationConfig] = {
    ConfigSource.file(path).load[ApplicationConfig]
  }

  def load: Either[ConfigReaderFailures, ApplicationConfig] = {
    ConfigSource.default.load[ApplicationConfig]
  }

}
