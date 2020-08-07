package com.vvittis

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.from_csv
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import java.util.regex.Pattern
import java.util.regex.Matcher
import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.log4j._
import org.apache.spark.sql.functions._




import java.util.regex.Pattern
import java.util.regex.Matcher
import java.text.SimpleDateFormat
import java.util.Locale

import Utilities._
object WordCount {

  case class LogEntry(ip: String, client: String, user: String, dateTime: String, request: String, status: String, bytes: String, referer: String, agent: String)
  val logPattern = apacheLogPattern()
  val datePattern = Pattern.compile("\\[(.*?) .+]")

  // Function to convert Apache log times to what Spark/SQL expects
  def parseDateField(field: String): Option[String] = {

    val dateMatcher = datePattern.matcher(field)
    if (dateMatcher.find) {
      val dateString = dateMatcher.group(1)
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val date = (dateFormat.parse(dateString))
      val timestamp = new java.sql.Timestamp(date.getTime());
      return Option(timestamp.toString())
    } else {
      None
    }
  }

  // Convert a raw line of Apache access log data to a structured LogEntry object (or None if line is corrupt)
  def parseLog(x:Row) : Option[LogEntry] = {

    val matcher:Matcher = logPattern.matcher(x.getString(0));
    if (matcher.matches()) {
      val timeString = matcher.group(4)
      return Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        parseDateField(matcher.group(4)).getOrElse(""),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)
      ))
    } else {
      return None
    }
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    //    val spark = SparkSession
    //      .builder
    //      .master("local[*]")
    //      .appName("ava Spark SQL basic example")
    //      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    //      .getOrCreate()
    //
    //    import spark.implicits._
    //
    //        val retailDataSchema = new StructType()
    //          .add("InvoiceNo", IntegerType)
    //          .add("Quantity", IntegerType)
    //          .add("Country", StringType)
    //
    //    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    //    val lines = spark
    //      .readStream
    //      .format("kafka")
    //      //.format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
    //      .option("kafka.bootstrap.servers", "localhost:9092")
    //      .option("subscribe", "topic2")
    //      .option("startingOffsets", "earliest")
    //      .load()
    //      .selectExpr("CAST(value AS STRING)")
    //      .select(functions.from_json($"value", retailDataSchema).as("data"))
    //    lines.printSchema()
    //
    //
    //
    //    // Generate running word count
    //    val wordCounts = lines
    ////      .select("data.Country")
    //      .groupBy("data.Country").sum("data.Quantity")
    //
    //
    //    val query = wordCounts.writeStream
    //      .outputMode("update")
    //      .format("console")
    //      .start()
    //
    //    query.awaitTermination()


    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ava Spark SQL basic example")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .config("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint")
      .getOrCreate()
    val rawData = spark
      .readStream
      .format("kafka")
      //.format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic3")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
    import spark.implicits._

    // Convert our raw text into a DataSet of LogEntry rows, then just select the two columns we care about
    val structuredData = rawData.flatMap(parseLog).select("status", "dateTime")

    // Group by status code, with a one-hour window.
    val windowed = structuredData.groupBy($"status", window($"dateTime", "1 hour")).count().orderBy("window")

    // Start the streaming query, dumping results to the console. Use "complete" output mode because we are aggregating
    // (instead of "append").
    val query = windowed.writeStream.outputMode("complete").format("console").start()

    // Keep going until we're stopped.
    query.awaitTermination()

  }
}
