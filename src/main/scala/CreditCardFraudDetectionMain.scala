import org.apache.spark.sql.SparkSession

object CreditCardFraudDetectionMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Detecting Fraudulent Credit Card Transactions").master("local[*]").getOrCreate()
    

  }
}
