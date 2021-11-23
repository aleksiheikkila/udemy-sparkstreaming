
package com.sundogsoftware.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

import java.util.concurrent._
import java.util.concurrent.atomic._

import java.time.LocalDateTime
import java.time.Duration

/** Monitors a stream of Apache access logs on port 9999, and prints an alarm
 *  if an excessive ratio of errors is encountered.
 */
object LogAlarmer {
  
  def main(args: Array[String]) {
    
    println("Printing command line args (if any)")
    for (arg <- args) {
      println(arg)
    }
    
    
    val ISSUE_ALARM_EVERY_X_SECS: Int = 30
    val MAX_TIME_SINCE_LAST_DATA: Int = 30  // if no data in 30secs, alarm
    
    var prev_alarm_issued_time = LocalDateTime.of(1970, 10, 4, 10, 20, 55)
    var prev_event_seen_time = LocalDateTime.now()
    
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogAlarmer", Seconds(1))
    
    setupLogging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    // Extract the status field from each log line
    val statuses = lines.map(x => {
          val matcher:Matcher = pattern.matcher(x); 
          if (matcher.matches()) matcher.group(6) else "[error]"
        }
    )
    
    // Now map these status results to success and failure
    val successFailure = statuses.map(x => {
      val statusCode = util.Try(x.toInt) getOrElse 0
      if (statusCode >= 200 && statusCode < 300) {
        "Success"
      } else if (statusCode >= 500 && statusCode < 600) {
        "Failure"
      } else {
        "Other"
      }
    })
    
    // Tally up statuses over a 5-minute window sliding every second
    val statusCounts = successFailure.countByValueAndWindow(Seconds(300), Seconds(1))
    // little bit simplet than reduceBy... we used before
    // Success/Failure/Other, and count for each
    
    // For each batch, get the RDD's representing data from our current window
    // WOuld this have only one RDD / partition?
    statusCounts.foreachRDD((rdd, time) => {  // go thru the DStream and each individual RDD in it
      // is one RDD ... the one countByValueAndWindow thing, consisting counts for the 300s window?
      // Keep track of total success and error codes from each RDD
      var totalSuccess:Long = 0
      var totalError:Long = 0

      if (rdd.count() > 0) {
        prev_event_seen_time = LocalDateTime.now()
        val elements = rdd.collect()  // collect results (from RDD) to plain old scala array
        for (element <- elements) {
          val result = element._1
          val count = element._2
          if (result == "Success") {
            totalSuccess += count
          }
          if (result == "Failure") {
            totalError += count
          }
        }
      }
      else {
        // No data
        // println("NO DATA")
        val time_since_last_data = Duration.between(prev_event_seen_time, LocalDateTime.now()).getSeconds()
        if (time_since_last_data > MAX_TIME_SINCE_LAST_DATA) {
          // alert
          
          val time_since_last_alarm = Duration.between(prev_alarm_issued_time, LocalDateTime.now()).getSeconds()
          if (time_since_last_alarm > ISSUE_ALARM_EVERY_X_SECS) {
             println("Wake somebody up! Something is horribly wrong. NO DATA!")
             prev_alarm_issued_time = LocalDateTime.now()    
             print(s"Alarm issued at: $prev_alarm_issued_time")
          }
        }
      }

      // Print totals from current window
      println("Total success: " + totalSuccess + " Total failure: " + totalError)
      
      // Don't alarm unless we have some minimum amount of data to work with
      if (totalError + totalSuccess > 100) {
        // Compute the error rate
        // Note use of util.Try to handle potential divide by zero exception
        val ratio:Double = util.Try( totalError.toDouble / totalSuccess.toDouble ) getOrElse 1.0
        // If there are more errors than successes, wake someone up
        if (ratio > 0.5) {
          val time_since_last_alarm = Duration.between(prev_alarm_issued_time, LocalDateTime.now()).getSeconds()
          
          if (time_since_last_alarm > ISSUE_ALARM_EVERY_X_SECS) {
             println("Wake somebody up! Something is horribly wrong.")
             prev_alarm_issued_time = LocalDateTime.now()    
             print(s"Alarm issued at: $prev_alarm_issued_time")
            
          }
          else {
            println(s"Alerting suppressed, prev alarm issued at $prev_alarm_issued_time")
          }
          // In real life, you'd use JavaMail or Scala's courier library to send an
          // email that causes somebody's phone to make annoying noises, and you'd
          // make sure these alarms are only sent at most every half hour or something.

        } else {
          println("All systems go.")
        }
      }
    })
    
    // Also in real life, you'd need to monitor the case of your site freezing entirely
    // and traffic stopping. In other words, don't use this script to monitor a real
    // production website! There's more you need.
    
    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

