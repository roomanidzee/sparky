package com.romanidze.sparky.relevantsites.processing

import java.net.URLDecoder
import java.net.IDN
import java.net.URL
import scala.util.matching.Regex

import com.romanidze.sparky.relevantsites.classes.Record

object DataLoader {

  def processRecord(input: String): Record = {

    val splitResult: Array[String] = input.split("\\t")

    val regexPattern = "(?:www\\.)?(.*)".r

    val uid: String = splitResult(0)
    val sourceURL = splitResult(2)

    if(sourceURL.startsWith("http") || sourceURL.startsWith("https")){
      val rawURL: String = URLDecoder.decode(splitResult(2), "UTF-8")
      val decodedURL: String = new URL(rawURL).getHost

      val finalURL: String = regexPattern.findFirstIn(decodedURL)
                                         .get
                                         .replace("www.", "")

      Record(uid, IDN.toASCII(finalURL))
    }else{
      Record(uid, "-")
    }

  }

}
