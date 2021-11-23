package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._

/** Listens to a stream of tweets and saves them to disk. */
object SaveTweets {
  // object in scala is basically the same thing as singleton in some other langs.
  // Makes sure there is some static instance of the SaveTweets that spark can call main on
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "SaveTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(1))
    // Batch size/interval of 1 sec
    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())  // DStream of tweet texts
    
    // Here's one way to just dump every partition of every stream to individual files:
    //statuses.saveAsTextFiles("Tweets", "txt")
    
    // But let's do it the hard way to get a bit more control.
    
    // Keep count of how many Tweets we've received so we can stop automatically
    // (and not fill up your disk!)
    var totalTweets: Long = 0
    var maxNumTweets: Int = 1000
    
    // Dont think about this in a procedural sequential manner!!
    // We define a workflow. Dont think this happening in order.
    // So for instance, foreachRDD is not gonna wait for the DStream to terminate.
    
    // access each individual RDD in the DStream as they come in
    // rdd, and timestamp
    statuses.foreachRDD((rdd, time) => {  // associate with an expression...
      // Don't bother with empty batches
      if (rdd.count() > 0) {
        // Combine each partition's results into a single RDD:
        // Consolidate the RDD down to single RDD (that might still be distributed)
        // Esp. important if running on a cluster and pushin to DB, for instance
        // repartition(1) also leads to data on disk being in a single part-00000 file (instead of many)
        //val repartitionedRDD = rdd.repartition(1).cache()  // or .persist() -- whats the difference?
        val repartitionedRDD = rdd.cache()
        // cache, to prevent from reprocessing
        
        // And print out a directory with the results.
        // Saves to many text files
        repartitionedRDD.saveAsTextFile("./Tweets/Tweets_" + time.milliseconds.toString)
        // Stop once we've collected 1000 tweets.
        totalTweets += repartitionedRDD.count()
        println("Tweet count: " + totalTweets)
        
        if (totalTweets > maxNumTweets) {
          println(s"Max number of tweets ({maxNumTweets}) reached, exiting")
          System.exit(0)  // not the most graceful way to exit...
        }
      }
    })
    
    // You can also write results into a database of your choosing, but we'll do that later.
    
    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("C:/checkpoint/")  // we dont really need/use that here
    // in real case, use some distributed / safe location to store the checkpoint
    // checkpoint can be used to restore the driver in case of a failure.
    
    ssc.start()
    ssc.awaitTermination()
  }  
}
