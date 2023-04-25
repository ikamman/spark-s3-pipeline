package com.spark.home.assignment

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

trait Processing {
  def process(input: DataFrame): DataFrame
}

object Processing {

  object FindKeyMaxValueOddCounts extends Processing {

    def process(input: DataFrame): DataFrame = input
      .groupBy("key", "value")
      .agg(count("*").as("count"))
      .filter(col("count") mod 2 !== 0)
      .select(col("key"), col("value"))
  }
}
