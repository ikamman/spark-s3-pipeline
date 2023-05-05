package com.spark.home.assignment

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

object DataWriter {

  def write(data: RDD[KeyValue], path: String)(implicit
      spark: SparkSession
  ): Unit = {
    import spark.implicits._

    data
      .toDF("key", "value")
      .write
      .option("header", true)
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .csv(path)
  }
}
