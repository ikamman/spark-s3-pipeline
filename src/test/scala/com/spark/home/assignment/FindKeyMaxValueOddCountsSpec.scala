package com.spark.home.assignment

import org.scalatest.flatspec._
import org.scalatest.matchers.should._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql.Row

import scala.jdk.CollectionConverters._

class FindKeyMaxValueOddCountsSpec
    extends AnyFlatSpec
    with SparkTestContext
    with Matchers {

  import Processing._

  "FindKeyMaxValueOddCounts" should "found odd key values counts" in {

    val df = session.createDataFrame(
      List(
        Row(1, 2),
        Row(1, 2),
        Row(1, 2),
        Row(1, 1),
        Row(1, 1),
        Row(1, 3),
        Row(1, 3),
        Row(2, 0),
        Row(2, 0),
        Row(2, 1),
        Row(2, 1),
        Row(2, 1),
        Row(3, 0),
        Row(3, 1),
        Row(3, 1)
      ).asJava,
      StructType(
        Array(
          StructField("key", IntegerType, false),
          StructField("value", IntegerType, false)
        )
      )
    )

    val result = FindKeyMaxValueOddCounts.process(df)

    result.collect() should contain theSameElementsAs Array(
      Row(1, 2),
      Row(2, 1),
      Row(3, 0)
    )
  }
}
