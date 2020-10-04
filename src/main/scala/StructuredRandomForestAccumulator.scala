import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Encoder, Row, SparkSession}
import org.apache.commons.math3.distribution.PoissonDistribution
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.control.Breaks.{break, breakable}

object StructuredRandomForestAccumulator {

  // Case class
  case class InputData(data: String, purposeId: Int, keyTuple: Int, idHT: Int)

  case class OutputState(listKeyTuple: List[Int], listRes: List[Int], listLabel: List[Int], listOfPurposeId : List[Int], weightTree:Double, idHT: Int)

  case class Result(keyTuple: Int, res: Int, label:Int, purposeId:Int, weightTree:Double, idT: Int)


  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface (checkpoint iff stateful operator is needed)
    val spark = SparkSession.builder()
      .appName("SparkStructuredStreamingExample")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint")
      .getOrCreate()

    //spark.sparkContext.defaultParallelism

    // Create a stream of kafka topic dumped into the testSource topic
    val rawData = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
    //option("minPartitions",10)

    //rawData.writeStream.option("truncate", "false").format("console").start()

    // For conversion to DataSet to row-byte
    implicit val encoder: Encoder[InputData] = org.apache.spark.sql.Encoders.product[InputData]
    val structuredData = rawData.flatMap {
      line: Row =>

        val fields = line.getString(0).trim.split(",")
        val data = line.getString(0).trim.split(",", line.getString(0).length - 1).dropRight(2).mkString(",")

        // Up to number of trees of random forest
        val list = new java.util.ArrayList[InputData]()
        for (x <- 0 to 1) { list.add(InputData(data, fields(fields.length - 2).toInt, fields(fields.length - 1).toInt, x)) }
        list.toSeq
    }

    //structuredData.writeStream.format("console").option("truncate", "false").start()

    // Test examples : set of attributes,purposeId=-5,keyTuple,idHT
    // Train examples : set of attributes,purposeId=5,keyTuple,idHT
    // Predict examples : set of attributes-label,purposeId=-10,keyTuple,idHT

    // Print schema
    structuredData.printSchema()

    import org.apache.spark.sql.{Encoder, Encoders}
    implicit val NodeEncoder: Encoder[HoeffdingTree] = Encoders.kryo[HoeffdingTree]
    import spark.implicits._

    // FlatMapGroupsWithState
    //repartition(2,col("randInt"))
    val result = structuredData.groupByKey(structuredData => structuredData.idHT).flatMapGroupsWithState[HoeffdingTree, OutputState](OutputMode.Update, GroupStateTimeout.ProcessingTimeTimeout) {

      case (idHT: Int, data: Iterator[InputData], state: GroupState[HoeffdingTree]) =>

        if (state.exists) {

          val hoeffding_tree = state.get
          val listRes = new java.util.ArrayList[Int]()
          val listKeyTuple = new java.util.ArrayList[Int]()
          val listLabel = new java.util.ArrayList[Int]()
          val listPurposeId = new java.util.ArrayList[Int]()

          while (data.hasNext) {

            val input = data.next()
            val purposeId = input.purposeId
            val inputString = input.data.split(",")
            val keyTuple = input.keyTuple

            breakable {

              // Testing and Predicted examples
              if (purposeId == -5 || purposeId == -10) {
                println("Find Test or Predicted example!!!")
                println("Input is :" + input)
                listRes.add(hoeffding_tree.TestHoeffdingTree(hoeffding_tree.root,inputString,purposeId))
                listKeyTuple.add(keyTuple)
                listPurposeId.add(purposeId)
                // Testing
                if(purposeId == -5) listLabel.add(inputString(inputString.length-1).toInt)
                // Predicted
                else listLabel.add(-1)
                break
              }

              // Training examples
              else{

                // Weighted votes based on tree's test-then-train accuracy

                // Testing(update the weight of tree)
                hoeffding_tree.TestHoeffdingTree(hoeffding_tree.root,inputString,purposeId)

                val poisson = new PoissonDistribution(6.0)
                val valueOfPoisson = poisson.sample

                // Repeat as many time as is the k-weight of Poisson distribution if condition is valid
                if (valueOfPoisson > 0) for (_ <- 0 until valueOfPoisson) { hoeffding_tree.UpdateHoeffdingTree(hoeffding_tree.root, inputString) }
                // else test???
              }
            }
          }
          state.update(hoeffding_tree)
          Iterator(OutputState(listKeyTuple.toList,listRes.toList,listLabel.toList,listPurposeId.toList,hoeffding_tree.getWeight,idHT))
        }
        else {

          println("Initialize state-HT")
          val listRes = new java.util.ArrayList[Int]()
          val listKeyTuple = new java.util.ArrayList[Int]()
          val listPurposeId = new java.util.ArrayList[Int]()
          val listLabel = new java.util.ArrayList[Int]()
          val listTestTuple = new java.util.ArrayList[String]()

          // Initialize Hoeffding tree
          val hoeffding_tree = new HoeffdingTree
          hoeffding_tree.CreateHoeffdingTree(5, 8,5,0.9,0.15)
          hoeffding_tree.print_m_features()

          // Parsing data
          while (data.hasNext) {

            val input = data.next()
            val purposeId = input.purposeId
            val keyTuple = input.keyTuple
            val inputString = input.data.split(",")

            // Keep testing tuples
            breakable {

              // Testing and Predicted tuples
              if (purposeId == -5 || purposeId == -10){
                println("Input is :" + input)
                listKeyTuple.add(keyTuple)
                listTestTuple.add(input.data)
                listPurposeId.add(purposeId)
                // Testing
                if(purposeId == -5) listLabel.add(inputString(inputString.length-1).toInt)
                // Predicted
                else listLabel.add(-1)
                break
              }

              // Training tuples
              else {

                // Weighted votes based on tree's test-then-train accuracy

                // Testing(update the weight of tree)
                hoeffding_tree.TestHoeffdingTree(hoeffding_tree.root,inputString,purposeId)

                // Training
                val poisson = new PoissonDistribution(6.0)
                val valueOfPoisson = poisson.sample

                // Repeat as many time as is the k-weight of Poisson distribution if condition is valid
                if (valueOfPoisson > 0) for (_ <- 0 until valueOfPoisson) { hoeffding_tree.UpdateHoeffdingTree(hoeffding_tree.root, inputString) }
                //else test???
              }
            }
          }
          state.update(hoeffding_tree)

          // Founded testing and predicted tuples
          if (listKeyTuple != null) {
            println("Size of listKeyTuple is " + listKeyTuple.size)
            for (i <- 0 until listKeyTuple.size) {
              listRes.add(hoeffding_tree.TestHoeffdingTree(hoeffding_tree.root, listTestTuple.get(i).split(","),listPurposeId.get(i)))
            }
            Iterator(OutputState(listKeyTuple.toList,listRes.toList,listLabel.toList,listPurposeId.toList,hoeffding_tree.getWeight,idHT))
          }
          else None.iterator

          //None.iterator

        }

    }

    // FlatMap on result
    val flatMapResult = result.filter(x => x != null).flatMap {
      result: OutputState =>
        val list = new java.util.ArrayList[Result]()
        for (i <- result.listRes.indices) { list.add(Result(result.listKeyTuple(i),result.listRes(i),result.listLabel(i),result.listOfPurposeId(i),result.weightTree,result.idHT)) }
        list.toSeq
    }

    // Print execution plan
    //flatMapResult.explain("formatted")

    // Write on kafka topic-result
    val kafkaResult = flatMapResult.map(flatMapResult =>
      flatMapResult.keyTuple.toString // key of tuple
        .concat(",").concat(flatMapResult.res.toString) // result of test
        .concat(",").concat(flatMapResult.label.toString) // label of tuple(only for testing tuples)
        .concat(",").concat(flatMapResult.purposeId.toString) // purposeId of tuple
        .concat(",").concat(flatMapResult.idT.toString)  // id of tree
        .concat(",").concat(flatMapResult.weightTree.toString)) //weight of tree
      .toDF("value")

    // Result : keyTuple,res,label,purposeId,idT,weightTree

    // Start the streaming query, dumping results to the topic results.
    val query = kafkaResult
      .selectExpr("CAST(value AS STRING)")
      .writeStream.outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "result")
      .queryName("TestStatefulOperator")
      .start()

    // Only for testing!!!
    //val query = kafkaResult.writeStream.outputMode("update").format("console").queryName("TestStatefulOperator").start()

    // Keep going until we're stopped
    query.awaitTermination()

    spark.stop()

  }

}
