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


  // grouping aggregation
  // grouping based on invoice number and country
  // calculate total quantity for each group
  // calculate sum of invoice value = quantity * unit price

  // column object expression

  val summaryDf = input.groupBy("Country", "InvoiceNo")
    .agg(sum("Quantity").as("Total Quantity"),
      sum(expr("Quantity * UnitPrice")).as("Invoice Value"))

  summaryDf.show()


  //  // simple aggregation
  //  // aggregation using column object expression
  //  input.select(
  //    count("*").as("row_count"),
  //    sum("Quantity").as("TotalQuantity"),
  //    avg("UnitPrice").as("AvgPrice"),
  //    countDistinct("InvoiceNo").as("DistinctInvoices")
  //  ).show()
  //
  //  // aggregation using string expression
  //
  //  input.selectExpr(
  //    "count(*) as RowCount", // count(*) counts the null values also
  //    "sum(Quantity) as TotalQuantity",
  //    "avg(UnitPrice) as AvgPrice",
  //    "count(Distinct(InvoiceNo)) as CountDistinct"
  //  ).show()
  //
  //  // aggregation using spark sql
  //
  //  input.createOrReplaceTempView("sales")
  //
  //  spark.sql("select count(*), sum(Quantity), avg(UnitPrice), count(distinct(InvoiceNo)) from sales").show()

  spark.stop()
}
