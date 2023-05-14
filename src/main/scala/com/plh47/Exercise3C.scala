package com.plh47

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, desc, explode}

object Exercise3C extends App {
  // create a spark session configuration
  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("Exercise3C")
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

  // split the title column of the modifiedDF dataframe into words using the empty space delimiter
  val wordsDF = modifiedDF.withColumn("words", functions.split(col("title"), " "))
    .select(explode(col("words")).as("word"))

  // filter out rows from wordsDF dataframe where the length of word is less than 4 characters
  val filterOutShortWordsDF = wordsDF.filter(functions.length(col("word")) >= 4)

  // filter out rows from filterOutShortWordsDF dataframe where word matches the pattern of non a four-digit year enclosed in parentheses
  val filterOutYearWordsDF = filterOutShortWordsDF.filter(!col("word").rlike("\\(\\d{4}\\)"))

  // group the rows in filterOutYearWordsDF dataframe by the word column and add counter
  val wordCountsDF = filterOutYearWordsDF.groupBy("word")
    .count()
    // order by descending mode in count column
    .orderBy(desc("count"))
    // filter out the records less than 10
    .filter(col("count") >= 10)

  // display the dataframe
  wordCountsDF.show()
}