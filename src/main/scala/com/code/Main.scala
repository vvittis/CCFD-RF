package com.code

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Main {


  case class Entry(timeStamp: String, Outlook: String, Temperature: String, Humidity: String, Class: String)

  class Entru

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

  def sumFunc(accum: String, n: String): String = {
    println("First" + accum)
    println("Second" + n)
    return ""
  }


  def mapFunction1(entry: Entry): Int = {

    //    var BaggingNumber:Int  = node._1
    //    var entry: Entry = node._2

    var timeStamp: String = entry.timeStamp
    var outlook: String = entry.Outlook
    var temperature: String = entry.Temperature
    var humidity: String = entry.Humidity
    var classof: String = entry.Class

    println("Hey Bill. I just received a Entry with " + timeStamp + " " + outlook + " " + temperature + " " + humidity + " " + classof + " ")
    println("...and it feels awesome")
    return 1
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
      //.format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic22")
      .option("startingOffsets", "earliest")
      .option("includeHeaders", "true")
      .load()
      .selectExpr("CAST(value AS STRING)")

    lines.printSchema()

    val byZipCode = (o: Entry) => o.Outlook
    /* STEP 1. Convert our raw text into a DataSet of Entry rows */
    val structuredData = lines.as[String].map(mapFunction)


      .groupByKey(byZipCode)







//      .withColumnRenamed("_1", "key").withColumnRenamed("_2", "value")

//      .withColumnRenamed("_1", "key")




//      .withColumnRenamed("_2", "value")
    //        structuredData.printSchema()
    //        structuredData.writeStream
    //          .format("console")
    //          //      .outputMode("complete")
    //          .option("truncate", "false")
    //          .start()

    //      .as[(Int,String)]
//    val data1 = structuredData.as[(Int, String)].reduce((x, y) => (x + "   ", y + "  "))



    structuredData.writeStream
              .format("console")
              //      .outputMode("complete")
              .option("truncate", "false")
              .start()
              .awaitTermination()


    //    val reducedData = structuredData.groupBy("value._2.Outlook").agg(count("value._1"))
    //    reducedData
    //      .writeStream
    //      .format("console")
    //      .outputMode("complete")
    //      .option("truncate", "false")
    //      .start()
    //      .awaitTermination()


  }
}


