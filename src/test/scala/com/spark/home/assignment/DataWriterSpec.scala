package com.spark.home.assignment

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataWriterSpec extends AnyFlatSpec with SparkTestContext with Matchers {

  "DataWriter" should "write DataFrame into proper TSV file" in {

    import session.implicits._

    val path = "target/test.tsv"

    val data = session.sparkContext.parallelize(
      List(
        (0, 1),
        (1, 3),
        (2, 5),
        (3, 2),
        (4, 7)
      )
    )

    noException shouldBe thrownBy(
      DataWriter.write(data, path)
    )

    val toTest = DataReader.read(path, "\t")

    toTest.collect() shouldBe data.collect()
  }
}
