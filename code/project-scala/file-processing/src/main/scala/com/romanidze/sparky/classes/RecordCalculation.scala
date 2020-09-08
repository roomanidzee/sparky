package com.romanidze.sparky.classes

import tethys.{JsonReader, JsonWriter}

/**
 * Case Class for histogramm representation
 * @param histFilm histogram for input film
 * @param histAll histogram for all films
 * @author Andrey Romanov
 */
case class RecordCalculation(histFilm: Seq[Int], histAll: Seq[Int])

object RecordCalculation{

  implicit val reader: JsonReader[RecordCalculation] = JsonReader.builder
    .addField[Seq[Int]]("hist_film")
    .addField[Seq[Int]]("hist_all")
    .buildReader(RecordCalculation.apply)

  implicit val writer: JsonWriter[RecordCalculation] = JsonWriter.obj[RecordCalculation]
    .addField("hist_film")(_.histFilm)
    .addField("hist_all")(_.histAll)

}
