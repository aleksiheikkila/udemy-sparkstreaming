

package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import Utilities._

/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {
  // Basic pattern. PrintTweets + main
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
    // PrintTweets is the name shown in e.g. monitoring
    // Seconds(1) - process 1 sec worth of data each time (accumulate). New data chunk every second.
    // One second batch interval
    
    // There is another more recent approach, structured streaming presented later
    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    // TwitterUtils is part of the spark examples...
    val tweets = TwitterUtils.createStream(ssc, None)  // create DStream
    // This receives raw information from twitter in DStream abstraction... of status objects
    
    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())  // Generates new DStream of tweet text
    
    // Print out the first ten. Calling print on the DStream itself.
    statuses.print()  // if you want to print all of them, probably call .collect() first...
    
    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }  
}