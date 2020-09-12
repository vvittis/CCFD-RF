package com.code

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}

object Main {

  case class Entry(random_number: Int, Id: Int, Name: String, Age: Int, Friends: Int)


  case class StoredInfo(Counter: Int)

  case class UpdatedInfo(random_number: Int, Counter: Int, expired: Boolean)

  def favoriteDonut(): Int = {
    val start = 0
    val end = 10
    val rnd = new scala.util.Random
    return start + rnd.nextInt((end - start) + 1)
  }

  def mapFunction(line: String): (Int, String) = {
    var fields = line.split(",")
    //    return (favoriteDonut(), Entry(fields(0).trim, fields(1).trim, fields(2).trim, fields(3).trim, fields(4).trim))
    return (favoriteDonut(), fields(1).trim)
  }

  object Entry {
    def apply(rawStr: String): Entry = {
      val fields = rawStr.split(",")
      Entry(77, fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ava Spark SQL basic example")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .config("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    import spark.implicits._
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic2")
      .option("startingOffsets", "earliest")
      .load()


    lines.printSchema()

    /* STEP 1. Convert our raw text into a DataSet of Entry rows */
    //    val structuredData = lines.selectExpr("CAST(value AS STRING)").as[String].map(mapFunction)


    val entries: Dataset[Entry] = lines.selectExpr("CAST(value AS STRING)")
      .map(r ⇒ Entry(r.getString(0)))

    entries.printSchema()
    val query1 = entries.writeStream
      .format("console")
      //      .outputMode("append")
      .option("truncate", "false")
      .start()
    val entries_state = entries.groupByKey(x => x.random_number).mapGroupsWithState[StoredInfo, UpdatedInfo](GroupStateTimeout.ProcessingTimeTimeout) {

      case (random_number: Int, entries: Iterator[Entry], state: GroupState[StoredInfo]) =>
        if (state.hasTimedOut) {
          val finalUpdate = UpdatedInfo(random_number, state.get.Counter, expired = true)
//          state.remove()
          finalUpdate
        }
        else {
          var sum = 0
          if (!state.exists) {
            sum = 0
          }
          else {
            sum = state.get.Counter
          }

          for (k <- entries) {
            println("Random number " + random_number + "  Friends " + k.Friends)
            sum = sum + k.Friends
          }
          state.update(StoredInfo(sum))
          //          println("1 -> " + state.exists)
          state.setTimeoutDuration("10 seconds")

          UpdatedInfo(random_number, state.get.Counter, expired = false)
        }


    }

    val query = entries_state
      .writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      //      .trigger(Trigger.Continuous("1 second"))
      .start()
    query1.awaitTermination()
    query.awaitTermination()
  }
}


