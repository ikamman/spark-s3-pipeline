package com.spark.home.assignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark._

object S3App extends App {
  val argConf = new ArgConfig(args)

  implicit val spark =
    SparkSession
      .builder()
      .appName("S3 App")
      .getOrCreate()

  if (!argConf.local.getOrElse(false)) {
    val credentials = CredentialsHelper.load(
      argConf.credentials.toOption,
      argConf.profile.toOption
    )
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.access.key", credentials.getAWSAccessKeyId())
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.secret.key", credentials.getAWSSecretKey())
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext.hadoopConfiguration
      .set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
  }

  import Processing._

  val tsvs = DataReader.read(s"${argConf.input()}/*.tsv", "\t")
  val csvs = DataReader.read(s"${argConf.input()}/*.csv", ",")

  val result = FindKeyMaxValueOddCounts.process(tsvs.union(csvs))

  DataWriter.write(result, argConf.output())

  spark.stop()
}
