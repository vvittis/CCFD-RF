import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Encoder, Row, SparkSession}
import org.apache.commons.math3.distribution.PoissonDistribution
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.control.Breaks.{break, breakable}

object StructuredRandomForestAccumulator{

  // Case class
  case class InputData(data:String,keyTuple:Int,idHT:Int)

  case class OutputState(listKeyTuple :List[Int],listRes :List[Int],idHT:Int)

  case class Result(keyTuple:Int,res:Int,idT:Int)


  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface(checkpoint iff stateful operator is needed)
    val spark = SparkSession.builder()
      .appName("SparkStructuredStreamingExample")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint")
      .getOrCreate()

    //spark.sparkContext.defaultParallelism

    // Create a stream of kafka topic dumped into the testSource topic
    val rawData = spark.readStream
      .format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testSource")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
    //option("minPartitions",10)

    // For conversion to DataSet to row-byte
    implicit val encoder: Encoder[InputData] = org.apache.spark.sql.Encoders.product[InputData]
    val structuredData = rawData.flatMap{
      line: Row =>

        val fields = line.getString(0).trim.split(",")
        val data = line.getString(0).trim.split(",",6).dropRight(1).mkString(",")

        // Up to number of trees of random forest
        val list = new java.util.ArrayList[InputData]()
        for (x <- 0 to 1){ list.add( InputData(data,fields(fields.length-1).toInt,x) ) }
        list.toSeq

    }

    // Test examples : set of attributes(5),keyTuple,idHT
    // Train examples : set of attributes(5),keyTuple,idHT

    // Print schema
    structuredData.printSchema()

    import org.apache.spark.sql.{Encoder, Encoders}
    implicit val NodeEncoder: Encoder[Node] = Encoders.kryo[Node]
    import spark.implicits._

    // FlatMapGroupsWithState
    //repartition(2,col("randInt"))
    val result = structuredData.groupByKey(structuredData => structuredData.idHT).flatMapGroupsWithState[Node,OutputState](OutputMode.Update,GroupStateTimeout.ProcessingTimeTimeout){

      case( idHT: Int , data : Iterator[InputData] , state:GroupState[Node]) =>

        if(state.exists){

          val root = state.get.FindRoot(state.get)
          val listRes = new java.util.ArrayList[Int]()
          val listKeyTuple = new java.util.ArrayList[Int]()
          //Get the random subset of attributes

          while(data.hasNext){
            val input = data.next()
            val stringInput = input.data.split(",")

            breakable {
              // Testing examples
              if (stringInput(1).toInt == 47) {
                println("Find Test example!!!")
                println("Input is :" + input)
                listRes.add(state.get.FindRoot(state.get).TestHT(state.get.FindRoot(state.get), stringInput))
                listKeyTuple.add(input.keyTuple)
                break
              }
              // Training examples
              else {
                val poisson = new PoissonDistribution(6.0)
                val valueOfPoisson = poisson.sample
                // Repeat as many time as is the k-weight of Poisson distribution if condition is valid
                if( valueOfPoisson > 0 ) { for(_ <- 0 to valueOfPoisson){ root.UpdateHT(root, stringInput) } }
                // else test???
              }
            }
          }

          state.update(root)
          Iterator( OutputState(listKeyTuple.toList,listRes.toList,idHT) )
        }
        else{

          println("Initialize state-HT")
          val listRes = new java.util.ArrayList[Int]()
          val listKeyTuple = new java.util.ArrayList[Int]()
          val listTestTuple = new java.util.ArrayList[String]()
          //Get the random subset of attributes

          // Initialize Hoeffding tree
          val root = new Node
          root.CreateHT()

          // Parsing data
          while(data.hasNext){
            val input = data.next()
            val stringInput = input.data.split(",")

            // Keep testing tuples
            breakable{
              // Testing tuples
              if (stringInput(1).toInt == 47){
                println("Input is :" + input)
                listKeyTuple.add(input.keyTuple)
                listTestTuple.add(input.data)
                break
              }
              // Training tuples
              else{
                val poisson = new PoissonDistribution(6.0)
                val valueOfPoisson = poisson.sample
                // Repeat as many time as is the k-weight of Poisson distribution if condition is valid
                if( valueOfPoisson > 0 ) { for(_ <- 0 to valueOfPoisson){ root.UpdateHT(root, stringInput) } }
                //else test???
              }
            }
          }
          state.update(root)

          // Founded test tuples
          if(listKeyTuple != null){
            println("Size of listKeyTuple is "+listKeyTuple.size)
            for(i <- 0 until listKeyTuple.size){ listRes.add(root.FindRoot(root).TestHT(root,listTestTuple.get(i).split(","))) }
            Iterator( OutputState(listKeyTuple.toList,listRes.toList,idHT) )
          }
          else{ None.iterator }

          //None.iterator

        }

    }

    // FlatMap on result
    val flatMapResult = result.filter(x => x != null).flatMap{
      result: OutputState =>
        val list = new java.util.ArrayList[Result]()
        for (i <- result.listRes.indices) { list.add(Result(result.listKeyTuple(i), result.listRes(i), result.idHT)) }
        list.toSeq
    }

    // Print execution plan
    //flatMapResult.explain("formatted")

    // Write on kafka topic-result
    val kafkaResult = flatMapResult.map(flatMapResult => flatMapResult.keyTuple.toString.concat(",").concat(flatMapResult.res.toString).concat(",").concat(flatMapResult.idT.toString)).toDF("value")

    // Start the streaming query, dumping results to the topic results.
    val query = kafkaResult.selectExpr("CAST(value AS STRING)").writeStream.outputMode("update").format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "result").queryName("TestStatefulOperator").start()

    // Only for testing!!!
    //val query = kafkaResult.writeStream.outputMode("update").format("console").queryName("TestStatefulOperator").start()

    // Keep going until we're stopped
    query.awaitTermination()

    spark.stop()

  }

}
