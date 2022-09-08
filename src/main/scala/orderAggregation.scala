import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object orderAggregation extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "data aggregation example")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val input = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "/Users/vigneishn/Downloads/order_data-201025-223502 (1).csv")
    .load()


  // aggregation using column object expression
  input.select(
    count("*").as("row_count"),
    sum("Quantity").as("TotalQuantity"),
    avg("UnitPrice").as("AvgPrice"),
    countDistinct("InvoiceNo").as("DistinctInvoices")
  ).show()

  spark.stop()
}
