package com.spark.home.assignment

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.spark.sql.Row

import scala.jdk.CollectionConverters._

class DataWriterSpec extends AnyFlatSpec with SparkTestContext with Matchers {

  val expectedSchema = StructType(
    Array(
      StructField("key", IntegerType, true),
      StructField("value", IntegerType, true)
    )
  )

  "DataWriter" should "write DataFrame into proper TSV file" in {

    import session.implicits._

    val path = "target/test.tsv"

    val df = session.createDataFrame(
      rows = List(
        Row(0, 1),
        Row(1, 3),
        Row(2, 5),
        Row(3, 2),
        Row(4, 7)
      ).asJava,
      schema = expectedSchema
    )

    noException shouldBe thrownBy(
      DataWriter.write(df, path)
    )

    val toTest = DataReader.read(path, "\t")

    toTest.collect() shouldBe df.collect()
  }
}
