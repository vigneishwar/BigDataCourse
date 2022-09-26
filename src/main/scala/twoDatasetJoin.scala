import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object twoDatasetJoin extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "join 2 datasets")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val orderDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "/Users/vigneishn/Downloads/orders-201025-223502 (2).csv")
    .load()



  // to fix ambiguos cols rename the col name before joining using .withColumnRenamed()
  val ordersNew = orderDf.withColumnRenamed("customerid", "cust_id")
  val customerDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "/Users/vigneishn/Downloads/customers-201025-223502 (1).csv")
    .load()

  val joinedDf = ordersNew.join(customerDf,orderDf.col("order_customer_id") === customerDf.col("customer_id"),"outer").sort("order_customer_id")
    .select("order_id","customer_id", "customer_fname").withColumn("order_id", expr("coalesce(order_id,-1)")) // replace null values using coalesce

  joinedDf.show(1000)


  scala.io.StdIn.readLine()
  spark.stop()
}
