package com.plh47

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, regexp_extract}

object Exercise3B extends App {
  // create a spark session configuration
  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("Exercise3B")
    .getOrCreate()

  // input file path
  val fileName = "data/movies.csv"

  // define the input type
  val df = sparkSession.read.format("csv")
    // set the first line as header
    .option("header", "true")
    // allow spark to map data to column
    .option("inferSchema", "true")
    // load the input from path
    .load(fileName)

  // filters
  var modifiedDF = df
    // filter out records with empty title
    .filter(col("title").isNotNull)

  // regular expression pattern to match a year enclosed in parentheses"
  val yearPattern = "\\((\\d{4})\\)"

  // extract the year from the title column using the regular expression and store it in a new column named year
  val extractedYear = regexp_extract(col("title"), yearPattern, 1).as("year")

  // create a new dataframe extractedDF by adding the year column
  val extractedDF = modifiedDF.withColumn("year", extractedYear)

  // create new dataframe based on extractedDF and add year column
  val aggregatedDF = extractedDF
    .withColumn("year", extractedYear)
    // group by year
    .groupBy("year")
    // count aggregated values and add the result to the new column called movie_count
    .agg(count("*").as("movie_count"))
    // order the dataframe by movie_count descending mode
    .orderBy(col("movie_count").desc)
    // get only the first 10 records
    .limit(10)

  // display the dataframe
  aggregatedDF.show()
}
