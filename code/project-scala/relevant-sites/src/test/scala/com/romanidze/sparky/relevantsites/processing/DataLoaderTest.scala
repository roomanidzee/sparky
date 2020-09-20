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
      Record(298563565220L, "smotri.com"),
      Record(298563565220L, "smotri.com"),
      Record(119952932415L, "echo.msk.ru"),
      Record(119952932415L, "echo.msk.ru"),
      Record(300632579775L, "russianfood.com"),
      Record(300632579775L, "russianfood.com"),
      Record(296539911524L, "sp.krasmama.ru"),
      Record(296539911524L, "sp.krasmama.ru"),
      Record(271793150671L, "forum.omskmama.ru"),
      Record(0L, "-"),
      Record(274060379972L, "vk.com"),
      Record(0L, "-"),
      Record(296539911524L, "sp.krasmama.ru"),
      Record(296539911524L, "sp.krasmama.ru"),
      Record(300762654387L, "zakon.kz"),
      Record(0L, "-"),
      Record(213567473327L, "zakon.kz"),
      Record(0L, "-"),
      Record(317095591614L, "web-ip.ru"),
      Record(317095591614L, "web-ip.ru"),
      Record(317095591614L, "пддонлайнэкзамен.рф")
    )

    val fileOutput: Vector[Record] = sourceIterator.map(elem => DataLoader.processRecord(elem))
                                                   .toVector

    fileOutput zip expectedOutput foreach(elem => {
      elem._1 shouldBe elem._2
    })

    source.close()

  }

}
