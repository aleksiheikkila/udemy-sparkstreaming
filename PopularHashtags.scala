package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._

/** Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 */
object PopularHashtags {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    val BATCH_INTERVAL_S: Int = 1
    val SLIDE_INTERVAL_S: Int = 1
    val WINDOW_S: Int = 600
    // include only words that start with PREFIX:
    val PREFIX: String = "$" // "#"  # stock tickers, hashtags?
    
    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(BATCH_INTERVAL_S))
    // batch interval 1 secs
    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())
    
    // Blow out each word into a new DStream
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    
    // Now eliminate anything that's not a hashtag
    val hashtags1 = tweetwords.filter(word => word.startsWith(PREFIX)).filter(word => word.length > 1)
    //val hashtags = hashtags1.filter(word => !word.matches(".*\\d.*"))  // get rid of those containing numbers...
    val hashtags = hashtags1.filter(word => !word.exists(_.isDigit))

    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag.toUpperCase, 1))
    
    // Now count them up over a 5 minute window sliding every one second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(WINDOW_S), Seconds(SLIDE_INTERVAL_S))
    // the seconds part is for removal... some kind of an optimization.
    // These are reduce func and its inverse. WHY?
    /**
    The inverse reduce function is used for optimising sliding window performance. 
    When the window duration is 300s and the interval duration is 1s, 
    a new reduced value can be computed from the previous reduced value by subtracting 1s of old data 
    that falls off from the new window and adding one second of new data. 
    There is also a version of reduceByKeyAndWindow without the inverse function which can be used 
    when the function is not invertible.
    **/
    
    // reduceByKeyAndWindow produces a DStream with a single RDD in it!
    // So it's ok to transform it below... Could also repartition?
    
    //  You will often see this written in the following shorthand:
    //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
    
    // Sort the results by the count values
    // transform every rdd - we have only one!
    // sort by second value, ie the count. false = descending?
    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
    // Print the top 10
    sortedResults.print
    
    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}
