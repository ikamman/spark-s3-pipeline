package com.spark.home.assignment

import org.apache.spark.sql._

object DataWriter {

  def write(data: DataFrame, path: String): Unit =
    data.write
      .option("header", true)
      .option("delimiter", "\t")
      .mode(SaveMode.Overwrite)
      .csv(path)
}
