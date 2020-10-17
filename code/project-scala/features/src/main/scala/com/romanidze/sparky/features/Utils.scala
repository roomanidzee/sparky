package com.romanidze.sparky.features

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object Utils {

  private def getCalendarValue(timestamp: Long): Calendar = {

    val formatter = new SimpleDateFormat("yyyyMMdd hh:mm:ss")
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"))
    val formattedTime: String = formatter.format(timestamp)

    val parsedDate: Date = formatter.parse(formattedTime)

    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(parsedDate)

    calendar

  }

  val getDayValue: UserDefinedFunction = udf { (timestamp: Long) =>
    val calendar: Calendar = getCalendarValue(timestamp)
    calendar.get(Calendar.DAY_OF_WEEK)
  }

  val getHourValue: UserDefinedFunction = udf { (timestamp: Long) =>
    val calendar: Calendar = getCalendarValue(timestamp)
    calendar.get(Calendar.HOUR_OF_DAY)
  }

}
