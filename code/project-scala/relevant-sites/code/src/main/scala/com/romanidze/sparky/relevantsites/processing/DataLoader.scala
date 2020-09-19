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
    val sourceURL = URLDecoder.decode(splitResult(2), "UTF-8")

    if(sourceURL.startsWith("http") || sourceURL.startsWith("https")){
      val decodedURL: String = new URL(sourceURL).getHost

      val host: String = regexPattern.findFirstIn(decodedURL)
                                     .get

      if(host.startsWith("www")){
        Record(uid, host.replace("www.", ""))
      }else{
        Record(uid, host)
      }
    }else{
      Record(uid, "-")
    }

  }

}
