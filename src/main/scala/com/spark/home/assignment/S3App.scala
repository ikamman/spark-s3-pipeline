package com.spark.home.assignment

import org.apache.spark.sql.SparkSession

object S3App extends App {
  val argConf = new ArgConfig(args)

  implicit val spark =
    SparkSession
      .builder()
      .appName("S3 App")
      .getOrCreate()

  if (!argConf.local.getOrElse(false)) {
    val credentials = Credentials.load(
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
  import DataReader._
  import DataWriter._

  val tsvs = read(s"${argConf.input()}/*.tsv", "\t")
  val csvs = read(s"${argConf.input()}/*.csv", ",")

  val result = findMaxOddValueCountForKey(tsvs.union(csvs))

  write(result, argConf.output())

  spark.stop()
}
