package com.romanidze.sparky.cli

import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
 * Config for CLI arguments
 * @param arguments arguments from console
 * @author Andrey Romanov
 */
class CLIConfig(arguments: Seq[String]) extends ScallopConf(arguments){

  val inputPath: ScallopOption[String] = opt[String](required = true, name = "inputPath")
  val outputPath: ScallopOption[String] = opt[String](required = true, name = "outputPath")
  val filmID: ScallopOption[String] = opt[String](required = true, name = "filmID")

  verify()

}
