package com.romanidze.sparky.classes

import tethys.{JsonReader, JsonWriter}

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
