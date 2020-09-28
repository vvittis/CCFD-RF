import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Encoder, Encoders, KeyValueGroupedDataset, Row, SparkSession}
import org.apache.log4j._
import java.util.UUID

import org.apache.spark.metrics.source.DoubleAccumulatorSource
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.{col, current_timestamp, monotonically_increasing_id, window}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.control.Breaks.{break, breakable}

object StructuredRandomForestAccumulator {

  // Case class
  case class Data(int1:Int, int2:Int, int3:Int, int4:Int, label:Int, randInt:Int, keyData:Double ,timestamp: Timestamp)

  case class OutputState(listID :List[Double],listRes :List[Int], listTimestamp: List[Timestamp], idHT:Int)

  case class Result(keyTuple:Double,res:Int,timestamp: Timestamp,idT:Int)

  case class IntermediateStateAggr(var countF:Int, var countNF:Int)

  class MyAggregate[IN](val f: IN => Result) extends Aggregator[IN,IntermediateStateAggr,String]{

    def zero: IntermediateStateAggr = IntermediateStateAggr(0,0)

    def reduce(isa: IntermediateStateAggr, a: IN): IntermediateStateAggr = {
      if(f(a).res == 0){ isa.countNF += 1 }
      else{ isa.countF += 1 }
      isa
    }

    def merge(b1: IntermediateStateAggr, b2: IntermediateStateAggr): IntermediateStateAggr = {
      IntermediateStateAggr(b1.countF+b2.countF,b1.countNF+b2.countNF)
    }

    def finish(reduction: IntermediateStateAggr): String = {
      if( reduction.countF > reduction.countNF){ "Fraud" }
      else{ "Not-Fraud" }
    }

    // Transform the output of the reduction
    def bufferEncoder: Encoder[IntermediateStateAggr] = Encoders.product
    def outputEncoder: Encoder[String] = Encoders.STRING

  }


  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface(checkpoint iff stateful operator)
    val spark = SparkSession.builder().appName("SparkStructuredStreamingExample").master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint").getOrCreate()


    // Create a stream of text files dumped into the fake directory
    val rawData = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testSource")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
    //.option("includeHeaders", "true")
    //text("number") , testing

    // For conversion to DataSet to row-byte
    implicit val encoder: Encoder[Data] = org.apache.spark.sql.Encoders.product[Data]
    val structuredData = rawData.flatMap{
      line: Row =>

        val time = new Timestamp(System.currentTimeMillis()) // TimeStamp
        val fields = line.getString(0).trim.split(",")
        val rand = scala.util.Random

        // Partition between build and test examples
        if (fields(1).toInt == 47){
          val list = new java.util.ArrayList[Data]()
          var newTime = time
          for (x <- 0 to 1){
            list.add(Data(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toInt, fields(4).toInt, x, fields(5).toDouble ,newTime))
            newTime = newTime.clone().asInstanceOf[Timestamp]
            newTime.setTime(newTime.getTime+60001)
          }
          //accum.add(1.0)
          list.toSeq
        }
        else{
          val person: Data = Data(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toInt, fields(4).toInt, rand.nextInt(2), fields(5).toDouble ,time)
          Seq(person)
        }
    }

    // Print schema
    structuredData.printSchema()

    import org.apache.spark.sql.{Encoder, Encoders}
    implicit val NodeEncoder: Encoder[Node] = Encoders.kryo[Node]
    import spark.implicits._

    // flatMapGroupsWithState
    val result = structuredData.groupByKey(structuredData => structuredData.randInt).flatMapGroupsWithState[Node,OutputState](OutputMode.Update,GroupStateTimeout.ProcessingTimeTimeout){
      case( randID: Int , data : Iterator[Data] , state:GroupState[Node]) =>
        if(state.exists){
          val root = state.get.FindRoot(state.get)
          val listRes = new java.util.ArrayList[Int]()
          val listID = new java.util.ArrayList[Double]()
          val listTimestamp = new java.util.ArrayList[Timestamp]()
          while(data.hasNext){
            val input = data.next()
            val stringInput = new Array[String](5)
            stringInput(0) = input.int1.toString
            stringInput(1) = input.int2.toString
            stringInput(2) = input.int3.toString
            stringInput(3) = input.int4.toString
            stringInput(4) = input.label.toString
            breakable {
              if (input.int2 == 47) {
                println("Find label!!!")
                listRes.add(state.get.FindRoot(state.get).TestHT(state.get.FindRoot(state.get), stringInput))
                listID.add(input.keyData)
                listTimestamp.add(input.timestamp)
                println("Key is " + input.keyData)
                println("Timestamp is " + input.timestamp)
                break
              }
              else { root.UpdateHT(root, stringInput) }
            }
          }
          state.update(root)
          Iterator( OutputState(listID.toList,listRes.toList,listTimestamp.toList,randID) )
        }
        else{
          println("Initialize state-HT")
          val listRes = new java.util.ArrayList[Int]()
          val listID = new java.util.ArrayList[Double]()
          val listTest = new java.util.ArrayList[Data]()
          val listTimestamp = new java.util.ArrayList[Timestamp]()
          // Initialize Hoeffding tree
          val root = new Node
          root.CreateHT()
          // Parsing data
          while(data.hasNext){
            val input = data.next()
            val stringInput = new Array[String](5)
            stringInput(0) = input.int1.toString
            stringInput(1) = input.int2.toString
            stringInput(2) = input.int3.toString
            stringInput(3) = input.int4.toString
            stringInput(4) = input.label.toString
            //Keep testing tuples
            breakable{
              if (input.int2 == 47){
                println("Find label on IS!!!")
                println("Key is " + input.keyData)
                println(input)
                listID.add(input.keyData)
                listTimestamp.add(input.timestamp)
                listTest.add(input)
                break
              }
              else{ root.UpdateHT(root, stringInput) }
            }
          }
          state.update(root)
          //Founded test tuples
          if(listID != null){
            println("Find label on IS")
            println("Size is "+listID.size())
            for(i <- 0 until listID.size()){
              val stringInput = new Array[String](5)
              stringInput(0) = listTest.get(i).int1.toString
              stringInput(1) = listTest.get(i).int2.toString
              stringInput(2) = listTest.get(i).int3.toString
              stringInput(3) = listTest.get(i).int4.toString
              stringInput(4) = listTest.get(i).label.toString
              listRes.add(root.FindRoot(root).TestHT(root,stringInput))
            }
            Iterator( OutputState(listID.toList,listRes.toList,listTimestamp.toList,randID) )
          }
          else{ None.iterator }
          //None.iterator
          //Iterator( OutputState(listID.toList,listRes.toList,listTimestamp.toList,randID) )
        }
    }

    // flatMap on result
    val flatMapResult = result.filter(x => x != null).flatMap{
      result: OutputState =>
        val list = new java.util.ArrayList[Result]()
        for (i <- result.listRes.indices) { list.add(Result(result.listID(i), result.listRes(i), result.listTimestamp(i), result.idHT)) }
        list.toSeq
    }

    // Groupby with watermark and apply based on flatMapGroupsWithState(tumbling or sliding window)
    val finalResult = flatMapResult.withWatermark("timestamp","1 minutes").groupBy(window(col("timestamp"),"3 minutes"),col("keyTuple")).count()
    // or groupBy(window($"timestamp","2 Minute","1 Minute"),$"listID")


    // Write the result as String(on text file)
    /*val finalResult = result.flatMap{
      case (line: OutputState) =>
        val str = line.listID.mkString(" ").concat(line.listRes.mkString(" ")).concat(line.total.toString)
        Seq(str)
    }.as[String]
    val query = finalResult.writeStream.format("text").option("path", "result").queryName("TestStatefulOperator").start()
    */

    // Start the streaming query, dumping results to the console. Use "complete" output mode because we are aggregating( instead of "append" mode)
    val query = finalResult.writeStream.format("console").queryName("TestStatefulOperator").start()
    //outputMode("update")

    // Write on kafka topic-result
    //val kafkaResult = flatMapResult.map(flatMapResult => flatMapResult.keyTuple.toString.concat(",").concat(flatMapResult.res.toString).concat(",").concat(flatMapResult.timestamp.toString).concat(",").concat(flatMapResult.idT.toString)).toDF("value")
    //key is optional
    //val query = kafkaResult.selectExpr("CAST(value AS STRING)").writeStream.outputMode("update").format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "result").queryName("TestStatefulOperator").start()

    // Keep going until we're stopped
    query.awaitTermination()

    spark.stop()

  }


}
