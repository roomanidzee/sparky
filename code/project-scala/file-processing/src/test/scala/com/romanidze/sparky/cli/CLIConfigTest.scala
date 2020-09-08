package com.romanidze.sparky.cli

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CLIConfigTest extends AnyFlatSpec with Matchers {

  it should "be parsed from command args" in {

    val args = Array(
      "--inputPath", "test",
      "--outputPath", "test",
      "--filmID", "42"
    )

    val cliConf = new CLIConfig(args)

    cliConf.inputPath.toOption.get shouldBe "test"

  }

}
