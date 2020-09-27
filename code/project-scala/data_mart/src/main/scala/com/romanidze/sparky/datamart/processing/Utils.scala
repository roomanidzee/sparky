package com.romanidze.sparky.datamart.processing

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Utils {

  def processURL(rawURL: Column): Column = {

    val parsedURL: Column = callUDF("parse_url", rawURL, lit("HOST"))

    regexp_replace(parsedURL, lit("www."), lit(""))

  }

  def processCategory(input: Column, prefix: String): Column = {

    val lowerCaseValue: Column = lower(input)

    val replacedValue1: Column = regexp_replace(lowerCaseValue, lit(" "), lit("_"))
    val replacedValue2: Column = regexp_replace(replacedValue1, lit("-"), lit("_"))

    concat(lit(prefix), replacedValue2)

  }

}
