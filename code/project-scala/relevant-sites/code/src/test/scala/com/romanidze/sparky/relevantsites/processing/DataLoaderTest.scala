package com.romanidze.sparky.relevantsites.processing

import java.net.URL
import java.nio.file.{Path, Paths}

import com.romanidze.sparky.relevantsites.classes.Record
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.BufferedSource

class DataLoaderTest extends AnyFlatSpec with Matchers{

  it should "process input string" in {

    val resourceURL: URL = getClass.getClassLoader.getResource("test_file.txt")
    val testFilePath: Path = Paths.get(resourceURL.toURI)

    val source: BufferedSource = scala.io.Source.fromFile(testFilePath.toString)
    val sourceIterator: Iterator[String] = source.getLines()

    val expectedOutput = Vector(
      Record("123", "cars.ru"),
      Record("123", "auto.ru"),
      Record("123", "ya.ru"),
      Record("456", "ya.ru"),
      Record("456", "auto.ru"),
      Record("456", "ya.ru"),
      Record("456", "market.ya.ru"),
      Record("789", "auto.ru"),
      Record("789", "cars.ru"),
      Record("789", "ya.ru"),
      Record("790", "xn--80aakahknigpedfdm6u.xn--p1ai")
    )

    val fileOutput: Vector[Record] = sourceIterator.map(elem => DataLoader.processRecord(elem))
                                                   .toVector

    fileOutput shouldBe expectedOutput

    source.close()

  }

}
