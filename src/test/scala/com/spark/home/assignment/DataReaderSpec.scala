package com.spark.home.assignment

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql.Row

import scala.jdk.CollectionConverters._

class DataReaderSpec extends AnyFlatSpec with SparkTestContext with Matchers {

  val expectedSchema = StructType(
    Array(
      StructField("key", IntegerType, true),
      StructField("value", IntegerType, true)
    )
  )

  "DataReader" should "read CSV file into proper DataFrame" in {

    val df = DataReader.read("test_data/test.csv", ",")

    df.schema shouldBe expectedSchema
    df.collect().forall(!_.anyNull) shouldBe true
  }

  it should "read TSV file into proper DataFrame" in {
    val df = DataReader.read("test_data/test.tsv", "\t")

    df.schema shouldBe expectedSchema
    df.collect().forall(!_.anyNull) shouldBe true
  }
}
