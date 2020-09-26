package com.romanidze.sparky.datamart.config

import java.net.URL
import java.nio.file.{Path, Paths}

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import pureconfig.error.ConfigReaderFailures

class ConfigurationLoaderTest extends AnyWordSpec with Matchers {

  "Configuration" should {

    "be parsed from file" in {

      val resourceURL: URL = getClass.getClassLoader.getResource("application.conf")
      val testFilePath: Path = Paths.get(resourceURL.toURI)

      val config: Either[ConfigReaderFailures, ApplicationConfig] =
        ConfigurationLoader.load(testFilePath.toString)

      config.isRight shouldBe true

    }

  }

}
