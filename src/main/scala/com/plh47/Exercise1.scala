package com.plh47

import org.apache.spark.sql.SparkSession

object Exercise1 extends App {
    // create a spark session configuration
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Exercise1")
      .getOrCreate()

    // input file path
    val fileName = "data/SherlockHolmes.txt"

    // create and spark context
    val sparkContext = sparkSession.sparkContext

    // load the text input to an RDD
    val wordsRDD = sparkContext.textFile(fileName)
      // replacement of all punctuation marks except apostrophe with " "
      .map(_.replaceAll("[^A-Za-z]+", " "))
      // turn all characters to lower case
      .map(_.toLowerCase)
      // split lines to words by using space separator
      .flatMap(_.split("[^a-zA-Z]+"))
      // filter out empty words
      .filter(_.nonEmpty)

    // reduce (aggregate) the dataset
    val wordCountsRDD = wordsRDD
      // map first word letter with word length
      .map(word => (word.charAt(0), (word.length, 1)))
      // sum up the values from the same key
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

    // Calculate the average word count for each letter
    val avgWordCountsRDD = wordCountsRDD
      // calculate the average word count for each letter
      .mapValues { case (sum, count) => sum.toDouble / count.toDouble }
      // sort the dataset by value in ascending mode
      .sortBy(_._2, ascending = false)


    avgWordCountsRDD.foreach(pair => {
      println(pair)
    })

    // save the result to file
    avgWordCountsRDD.saveAsTextFile("Exercise1_output")
}