package com.spark.home.assignment

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class FindKeyMaxValueOddCountsSpec
    extends AnyFlatSpec
    with SparkTestContext
    with Matchers {

  import Processing._

  "findMaxOddValueCountForKey" should "found odd key values counts" in {

    val rdd = session.sparkContext.parallelize(
      List(
        (1, 2),
        (1, 2),
        (1, 2),
        (1, 1),
        (1, 1),
        (1, 3),
        (1, 3),
        (2, 0),
        (2, 0),
        (2, 1),
        (2, 1),
        (2, 1),
        (3, 0),
        (3, 1),
        (3, 1)
      )
    )

    val result = findMaxOddValueCountForKey(rdd)

    result.collect() should contain theSameElementsAs List(
      (1, 2),
      (2, 1),
      (3, 0)
    )
  }
}
