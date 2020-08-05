package com.vvittis

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object WordCount {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ava Spark SQL basic example")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    import spark.implicits._

//    val retailDataSchema = new StructType()
//      .add("InvoiceNo", IntegerType)
//      .add("StockCode", IntegerType)
//      .add("Country", StringType)

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark
      .readStream
      .format("kafka")
      //      .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)","CAST(key AS STRING)")
      .as[(String,String)]


    // Generate running word count
    val wordCounts = lines.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()


    //    val filteredData = streamingData.filter("Country = 'United Kingdom'")
//    val query = filteredData.writeStream
//      .format("console")
//      .queryName("filteredByCountry")
//      .outputMode(OutputMode.Update())
//      .start()
//
//    query.awaitTermination()
  }
}
