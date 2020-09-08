package com.romanidze.sparky.processing

import better.files._

import java.net.URL
import java.nio.file.{Path, Paths}

import com.romanidze.sparky.classes.Record
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataCalculationTest extends AnyFlatSpec with Matchers {

  it should "make calculations" in {

    val resourceURL: URL = getClass.getClassLoader.getResource("test_file.txt")
    val testFilePath: Path = Paths.get(resourceURL.toURI)

    val processResult: List[Record] = DataLoader.loadData(testFilePath.toFile.toScala)
    val calculationResult: Seq[Int] = DataCalculation.calculateRatings(processResult)

    calculationResult shouldBe Seq(1, 1, 2)

  }

}
