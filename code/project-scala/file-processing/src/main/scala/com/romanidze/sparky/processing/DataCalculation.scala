package com.romanidze.sparky.processing

import com.romanidze.sparky.classes.Record

/**
 * Main record calculation
 *
 * @author Andrey Romanov
 */
object DataCalculation {

  def calculateRatings(input: List[Record]): Seq[Int] = {

    input.groupBy(_.ratingValue)
         .mapValues(_.size)
         .toSeq
         .sortBy(_._1)
         .map(elem => elem._2)

  }

}
