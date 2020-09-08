package com.romanidze.sparky.processing

import better.files._

import java.net.URL
import java.nio.file.{Path, Paths}

import com.romanidze.sparky.classes.Record
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataLoaderTest extends AnyFlatSpec with Matchers{

  it should "process input string" in {

    val testString = "196\t242\t3\t881250949"

    val processResult: Record = DataLoader.processString(testString)

    processResult.filmID shouldBe "242"
    processResult.ratingValue shouldBe "3"

  }

  it should "load data from file" in {

    val resourceURL: URL = getClass.getClassLoader.getResource("test_file.txt")
    val testFilePath: Path = Paths.get(resourceURL.toURI)

    val processResult: List[Record] = DataLoader.loadData(testFilePath.toFile.toScala)

    processResult.size shouldBe 4

    processResult(0) shouldBe Record("242", "3")
    processResult(1) shouldBe Record("302", "3")
    processResult(2) shouldBe Record("377", "1")
    processResult(3) shouldBe Record("51", "2")

  }

  it should "filter records" in {

    val inputRecords: List[Record] = List(Record("242", "3"), Record("302", "3"))

    val filteredData: List[Record] = DataLoader.filterData("242", inputRecords)

    filteredData.size shouldBe 1
    filteredData(0).ratingValue shouldBe "3"

  }

}
