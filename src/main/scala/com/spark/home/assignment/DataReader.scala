package com.spark.home.assignment

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.rdd.RDD

object DataReader {

  def read(path: String, delimiter: String)(implicit
      session: SparkSession
  ): RDD[KeyValue] = {

    import session.implicits._

    val rawDf = session.read
      .option("header", true)
      .option("delimiter", delimiter)
      .csv(path)
    rawDf
      .select(rawDf.columns.map(c => col(c).cast(IntegerType)): _*)
      .na
      .fill(0)
      .as[KeyValue]
      .rdd
  }
}
