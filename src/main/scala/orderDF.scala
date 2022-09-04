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

  val input = spark.read
    .option("header", true)
    .option("inferSchema", true) // never use inferSchema in production
    .csv("/Users/vigneishn/Downloads/orders-201019-002101.csv")

  val groupedOrders = input
    .repartition(4)
    .where("order_customer_id > 10000")
    .select("order_id", "order_customer_id")
    .groupBy("order_customer_id")
    .count()

  groupedOrders.show()
  scala.io.StdIn.readLine()
  spark.stop()
}
