package com.romanidze.sparky.relevantsites.processing

import java.net.URLDecoder
import java.net.IDN
import java.net.URL

import scala.util.matching.Regex
import com.romanidze.sparky.relevantsites.classes.Record

import scala.util.Try

object DataLoader {

  def processRecord(input: String): Record = {

    val splitResult: Array[String] = input.split("\\t")

    val regexPattern = "(?:www\\.)?(.*)".r

    val uid: String = splitResult(0)
    val rawURL : String = Try(splitResult(2)).getOrElse("-")
    val sourceURL: String = Try(URLDecoder.decode(rawURL, "UTF-8")).getOrElse("-")

    val cond: String => Boolean = (elem: String) => elem.equals("-") || elem.isEmpty

    if( ( sourceURL.startsWith("http") || sourceURL.startsWith("https") ) && !cond.apply(uid)){
      val decodedHost: String = new URL(sourceURL).getHost

      val host: String = regexPattern.findFirstIn(decodedHost)
                                     .get

      if(host.startsWith("www")){
        Record(uid.toLong, host.replace("www.", ""))
      }else{
        Record(uid.toLong, host)
      }
    }else{
      Record(0L, "-")
    }

  }

}
