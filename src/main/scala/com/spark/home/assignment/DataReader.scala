package com.spark.home.assignment

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataReader {

  val schema = StructType(
    Array(
      StructField("key", StringType, true),
      StructField("value", StringType, true)
    )
  )

  private[this] def asIntegerType(columnName: String): Column =
    when(col(columnName).isNull || col(columnName).isin("", " "), "0")
      .otherwise(col(columnName))
      .cast(IntegerType)

  def read(path: String, delimiter: String)(implicit
      session: SparkSession
  ): DataFrame = {
    val result = session.read
      .option("header", true)
      .option("delimiter", delimiter)
      .schema(schema)
      .csv(path)
      .withColumn("key", asIntegerType("key"))
      .withColumn("value", asIntegerType("value"))
    result
  }
}
