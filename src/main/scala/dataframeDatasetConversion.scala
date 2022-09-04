import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.sql.Timestamp

case class OrdersData (order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status: String)

object dataframeDatasetConversion extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "orders df")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val input: Dataset[Row] = spark.read
    .option("header", true)
    .option("inferSchema", true) // never use inferSchema in production
    .csv("/Users/vigneishn/Downloads/orders-201019-002101.csv")

  import spark.implicits._
  val ordersDs = input.as[OrdersData]
  ordersDs.filter(x => x.order_id<1000)


}
