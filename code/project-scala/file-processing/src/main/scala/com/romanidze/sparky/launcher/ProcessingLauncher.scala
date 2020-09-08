package com.romanidze.sparky.launcher

import com.romanidze.sparky.cli.CLIConfig

object ProcessingLauncher extends App{

  val conf = new CLIConfig(args)
  ProcessingInitializer.processInput(conf)

}
