package com.sundogsoftware.sparkstreaming
// declare the package we are in

// Import entire spark  library
import org.apache.spark._

/** Create a RDD of lines from a text file, and keep count of
 *  how often each word appears.
 */
object wordcount {
  // object, same name as the file?
  // main function is where this starts running
  
  def main(args: Array[String]) {
      // Set up a SparkContext named WordCount that runs locally using
      // all available cores.
      val conf = new SparkConf().setAppName("WordCount")  // name shown in dashboards etc
      conf.setMaster("local[*]")  // locally, on every available core
      val sc = new SparkContext(conf)
      // granddaddy of spark program, that creates RDDs etc

      // Create a RDD of lines of text in our book
      val input = sc.textFile("book.txt")  // every line in txt file to into an entry in input RDD
      // Use flatMap to convert this into an rdd of each word in each line
      val words = input.flatMap(line => line.split(' '))  // flatMap can create many entries for every entry
      // Convert these words to lowercase
      val lowerCaseWords = words.map(word => word.toLowerCase())
      // Count up the occurence of each unique word
      // Up until now, no computations have taken place. THis is an action that kicks off it all
      val wordCounts = lowerCaseWords.countByValue()
      // result are tuples (word, count)
      
      // Print the first 20 results
      val sample = wordCounts.take(20)
      
      for ((word, count) <- sample) {
        println(word + " " + count)
      }
      
      sc.stop()
    }  
}