package com.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object StructuredStreamingExamples {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder()
      .appName("Spark Structured Streaming")
      .master("local[*]")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")


    val retailDataSchema = new StructType()
      .add("Null",IntegerType)
      .add("InvoiceNo", IntegerType)
      .add("StockCode", IntegerType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", TimestampType)
      .add("UnitPrice", DoubleType)
      .add("CustomerId", IntegerType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)

    val streamingData = spark
      .readStream
      // .option("maxFilesPerTrigger","2")
      .schema(retailDataSchema)
      .csv("datasets/temp_working")

//     val filteredData = streamingData.filter("Country = 'United Kingdom'")
//    /*Just a Query*/
//     val query = filteredData.writeStream
//       .format("console")
//       .queryName("filteredByCountry")
//       .outputMode(OutputMode.Update())
//       .start()
//
//     query.awaitTermination()

    /*Basic Transformation*/

     val filteredData = streamingData
       .filter("Country = 'United Kingdom'")
       .where("Quantity > 1")
       .drop("CustomerId")
       .select("InvoiceNo","StockCode", "Description","Quantity")
       .writeStream
       .queryName("SalesDetails")
       .format("console")
       .outputMode(OutputMode.Update())
       .start()

     filteredData.awaitTermination()


    /*Basic Aggregations*/


    // val aggregateData = streamData
    //   .filter("Quantity > 10")
    //   .groupBy("InvoiceDate", "Country")
    //   .agg(sum("UnitPrice"))

    //   val aggregateQuery = aggregateData.writeStream
    //     .format("console") // represents sink to be used
    //     .outputMode(OutputMode.Append())
    //     .start()

    // aggregateQuery.awaitTermination()


    /*Tumbling Windowing*/

    //     val tumblingWindowAggregations = streamData
    //     .filter("Country = 'Spain'")
    //   .groupBy(
    //     window(col("InvoiceTimestamp"), "1 hours"),
    //     col("Country")
    //   )
    //   .agg(sum(col("UnitPrice")))

    // val sink = tumblingWindowAggregations
    //   .writeStream
    //   .format("console")
    //   .option("truncate", "false")
    //   .outputMode("complete")
    //   .start()

    // sink.awaitTermination()


    /*Sliding Window*/

    //     val tumblingWindowAggregations = streamData
    //   .filter("Country = 'Spain'")
    //   .groupBy(
    //     window(col("InvoiceTimestamp"), "1 hours", "15 minutes"),
    //     col("Country")
    //   )
    //   .agg(sum(col("UnitPrice")))

    // val sink = tumblingWindowAggregations
    //   .writeStream
    //   .format("console")
    //   .option("truncate", "false")
    //   .outputMode("complete")
    //   .start()

    // sink.awaitTermination()


    /*Triggers*/


    // val tumblingWindowAggregations = streamData
    //   .groupBy(
    //     window(col("InvoiceTimestamp"), "1 hours", "15 minutes"),
    //     col("Country")
    //   )
    //   .agg(sum(col("UnitPrice")))

    // val sink = tumblingWindowAggregations
    //   .writeStream
    //     .trigger(Trigger.Once())
    //   .format("console")
    //   .option("truncate", "false")
    //   .outputMode("complete")
    //   .start()

    // sink.awaitTermination()

  }
}
