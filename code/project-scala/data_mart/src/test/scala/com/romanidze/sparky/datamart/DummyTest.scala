package com.romanidze.sparky.datamart

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

class DummyTest extends AnyFunSuite with SharedSparkContext {

  override implicit def reuseContextIfPossible: Boolean = true

  test("test initializing spark context") {
    val numbers: List[Int] = List(1, 2, 3, 4)
    val rdd: RDD[Int] = sc.parallelize(numbers)

    assert(rdd.sum() === 10)
  }
}
