package com.romanidze.sparky.processing

import tethys._
import tethys.jackson._
import better.files.{File => ScalaFile}
import com.romanidze.sparky.classes.{Record, RecordCalculation}

object DataLoader {

  def processString(input: String): Record = {

    val splitResult: Array[String] = input.split("\\t")

    Record(splitResult(1), splitResult(2))

  }

  def loadData(input: ScalaFile): List[Record] = {

    val lines: Iterator[String] = input.lineIterator

    lines.map(elem => processString(elem))
         .toList

  }

  def filterData(filmID: String, data: List[Record]): List[Record] = data.filter(_.filmID == filmID)

  def writeData(calcResult: RecordCalculation, outputFile: ScalaFile): ScalaFile = outputFile.append(calcResult.asJson)

}
