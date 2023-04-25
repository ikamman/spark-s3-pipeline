package com.spark.home.assignment

import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

trait SparkTestContext {

  implicit val session: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Spark Unit Test")
    .getOrCreate()
}
