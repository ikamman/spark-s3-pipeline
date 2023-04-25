lazy val root = (project in file(".")).settings(
  organization := "com.spark.home.assignment",
  name := "s3-app",
  version := "0.1.0",
  scalaVersion := "2.13.10",
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", xs @ _*) =>
      MergeStrategy.filterDistinctLines
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case _                             => MergeStrategy.first
  },
  assembly / mainClass := Some("com.spark.home.assignment.S3App"),
  assembly / assemblyJarName := "s3-app.jar"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.0" % Provided,
  "org.apache.spark" %% "spark-core" % "3.4.0" % Provided,
  "org.apache.hadoop" % "hadoop-aws" % "3.2.2",
  // "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.316",
  "org.rogach" %% "scallop" % "4.1.0",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)
