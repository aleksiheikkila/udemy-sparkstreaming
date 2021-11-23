package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._
import java.util.concurrent._
import java.util.concurrent.atomic._
// concurrent for multi-threaded stuff to be safe!

// Modified from AverageTweetLength to get the max tweet length seen

/** Uses thread-safe counters to keep track of the average length of
 *  Tweets in a stream.
 */
object MaxTweetLength {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "AverageTweetLength" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))
    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())
    
    // Map this to tweet character lengths.
    // New DStream called lengths
    val lengths = statuses.map(status => status.length()) 
    // less data you retain and push around, the faster the spark will work
    
    // As we could have multiple processes adding into these running totals
    // at the same time, we'll just Java's AtomicLong class to make sure
    // these counters are thread-safe.
    var totalTweets = new AtomicLong(0)
    var totalChars = new AtomicLong(0)
    var batchMaxTweetLength: Int = 0  //= new AtomicLong(0)
    var globalMaxTweetLength: Int = 0  // new AtomicLong(0)
    // Java Long object that can let you add and access a Long integer value in thread safe manner!
    // There is also a more spark'y way of doing to wo resorting to Java objects...
    
    // In Spark 1.6+, you  might also look into the mapWithState function, which allows
    // you to safely and efficiently keep track of global state with key/value pairs.
    // We'll do that later in the course.
    
    lengths.foreachRDD((rdd, time) => {
      
      var count = rdd.count()
      if (count > 0) {
        totalTweets.getAndAdd(count)  // add to the AtomicLong
        
        totalChars.getAndAdd(rdd.reduce((x,y) => x + y))  // add up lengths... total number of chars in the RDD
        
        println("Total tweets: " + totalTweets.get() + 
            " Total characters: " + totalChars.get() + 
            " Average: " + totalChars.get() / totalTweets.get())
            
        batchMaxTweetLength = rdd.max()
        
        if (batchMaxTweetLength > globalMaxTweetLength) {
          globalMaxTweetLength = batchMaxTweetLength
        }
        
        println(s"Max tweet length in current batch: $batchMaxTweetLength, max seen so far: $globalMaxTweetLength")
      
      
      }
    })
    
    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
