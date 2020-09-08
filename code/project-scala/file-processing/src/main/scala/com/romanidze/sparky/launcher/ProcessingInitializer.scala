package com.romanidze.sparky.launcher

import java.nio.file.{Path, Paths}

import better.files._
import com.romanidze.sparky.classes.{Record, RecordCalculation}
import com.romanidze.sparky.cli.CLIConfig
import com.romanidze.sparky.processing.{DataCalculation, DataLoader}

/**
 * Initialization for all parameters
 * @author Andrey Romanov
 */
object ProcessingInitializer {

  def processInput(input: CLIConfig): Unit = {

    val filmID: String = input.filmID.toOption.get
    val inputPath: Path = Paths.get(input.inputPath.toOption.get)
    val outputPath: Path = Paths.get(input.outputPath.toOption.get)

    val records: List[Record] = DataLoader.loadData(
      Paths.get(inputPath.toString, "u.data").toFile.toScala
    )

    val histFilm: Seq[Int] = DataCalculation.calculateRatings(
      DataLoader.filterData(filmID, records)
    )

    val histAll: Seq[Int] = DataCalculation.calculateRatings(records)

    DataLoader.writeData(
      RecordCalculation(histFilm, histAll),
      Paths.get(outputPath.toString, "lab01.json").toFile.toScala
    )


  }

}
