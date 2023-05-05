PHONY: run-local

build:
	sbt clean assembly

run-local: build
	spark-submit \
  	--class "com.spark.home.assignment.S3App" \
  	--master "local[*]" \
  	target/scala-2.13/s3-app.jar --input ./test_data --output ./target/result.tsv --local