package com.plh47

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.functions._

object Exercise3A extends App {
  // create a spark session configuration
  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("Exercise3A")
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
    // filter out records with empty genres
    .filter(col("genres").isNotNull)

  // split the value from column by the pipe character
  val splitGenresDF = modifiedDF.withColumn("genres", split(col("genres"), "\\|"))

  // explode the split genres column to create multiple rows containing the genre value
  val explodedGenresDF = splitGenresDF.select(explode(col("genres")).as("genre"))

  // group the rows in explodedGenresDF by the genre column and calculate the count
  val genreCountDF = explodedGenresDF.groupBy("genre").count()

  // sort the genreCountDF dataFrame in ascending mode based on the genre column
  val sortedDF = genreCountDF.orderBy(col("genre").asc)

  // display the dataframe
  sortedDF.show()
}