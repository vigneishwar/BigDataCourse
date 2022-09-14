import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window

object windowAggr extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "window aggregation example")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val input = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "/Users/vigneishn/Downloads/windowdata-201025-223502 (1).csv")
    .load()

  // calculate the running total
  // paritioning by country, ordering by week num, use unboundedPreceeding to denote the first row
  val myWindow = Window.partitionBy("country").orderBy("weeknum").rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val total = input.withColumn("Running Total", functions.sum("invoicevalue").over(myWindow))
  total.show()
  spark.stop()

}
