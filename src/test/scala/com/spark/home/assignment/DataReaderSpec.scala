package com.spark.home.assignment

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataReaderSpec extends AnyFlatSpec with SparkTestContext with Matchers {

  "DataReader" should "read CSV file into proper RDD" in {
    val rdd =
      DataReader.read("test_data/test.csv", ",")

    rdd.count().toInt should be > 0
  }

  it should "read TSV file into proper RDD" in {
    val rdd =
      DataReader.read("test_data/test.tsv", "\t")

    rdd.count().toInt should be > 0
  }
}
