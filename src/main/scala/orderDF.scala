import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object orderDF extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "orders df")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
//
//  val input = spark.read
//    .format("csv")
//    .option("header", true)
//    .option("inferSchema", true) // never use inferSchema in production
//    .option("path", "/Users/vigneishn/Downloads/orders-201019-002101.csv")
//    .load
//    .csv("/Users/vigneishn/Downloads/orders-201019-002101.csv")


  val input = spark.read
    .format("json")
    .option("header", true)
    .option("path", "/Users/vigneishn/Downloads/players-201019-002101.json") // no need of inferSchema as JSON files infer the schema by default
    .option("mode", "FAILFAST")
    .load
//
//  val groupedOrders = input
//    .repartition(4)
//    .where("order_customer_id > 10000")
//    .select("order_id", "order_customer_id")
//    .groupBy("order_customer_id")
//    .count()

  input.show()
  scala.io.StdIn.readLine()
  spark.stop()

}
