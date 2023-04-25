# How to run the pipeline

All commands assume that spark is correctly installed and available on your `$PATH`

## Local pipeline run with provided test files

All test files are located in `./test_data` directory. To run the spark pipeling us the following commands:
```bash
# build project and produce fat jar file
sbt clean assembly

# submit spark joib using this command
spark-submit \
  	--class "com.spark.home.assignment.S3App" \
  	--master "local[*]" \
  	target/scala-2.13/s3-app.jar --input ./test_data --output ./target/result.tsv --local

```

## Local pipeline run with your data folder

```bash
# build project and produce fat jar file
sbt clean assembly

# submit spark joib using this command
spark-submit \
  	--class "com.spark.home.assignment.S3App" \
  	--master "local[*]" \
  	target/scala-2.13/s3-app.jar --input /your/input/data/directory --output /your/result/file/path.tsv --local

```

## Run pipeline on S3 bucket

First of all you need to private proper credentials in your credentials file located in `~/.aws/credentials`. By default the pipeling will use `default` profile.
If you want to use something other than default file use the option `--credentials`.
Full command below:

```bash
# submit spark joib using this command
spark-submit \
  	--class "com.spark.home.assignment.S3App" \
  	--master "local[*]" \
  	target/scala-2.13/s3-app.jar \
    --input s3n://data-processing-spark/input \
    --output s3n://data-processing-spark/output/result.tsv \
    --credentials ~/.aws/credentials

```

# Testing
Run
```bash
sbt test
```
to execute unit tests.
