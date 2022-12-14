import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}


object orderDF extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "orders df")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()
//
//  val input = spark.read
//    .format("csv")
//    .option("header", true)
//    .option("inferSchema", true) // never use inferSchema in production
//    .option("path", "/Users/vigneishn/Downloads/orders-201019-002101.csv")
//    .load
//    .csv("/Users/vigneishn/Downloads/orders-201019-002101.csv")

//
//  val input = spark.read
//    .format("json")
//    .option("header", true)
//    .option("path", "/Users/vigneishn/Downloads/players-201019-002101.json") // no need of inferSchema as JSON files infer the schema by default
//    .option("mode", "FAILFAST")
//    .load
//
//  val input = spark.read
//      .option("path", "/Users/vigneishn/Downloads/users-201019-002101.parquet") // by default spark uses parquet format
//      .load


    val input = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("path", "/Users/vigneishn/Downloads/orders-201025-223502 (1).csv")
      .load()

  spark.sql("create database if not exists retail")

  input.createOrReplaceTempView("orders") // spark sql

//  val resultDf = spark.sql("select order_status, count(*) as status_count from orders group by order_status order by status_count")
//
//  resultDf.show()
//
//  val resultDf = spark.sql("select order_customer_id, count(*) as total_orders from orders where order_status ='CLOSED'" +
//  "group by order_customer_id order by total_orders desc")
//
//  resultDf.show()

  // to save the file in the form of table
    input.write
      .format("csv") // by default it is parquet without .format()
      .mode(SaveMode.Overwrite)
      .saveAsTable("retail.orders")

  spark.catalog.listTables("retail").show()





// to save the file
//  input.write
//    .format("csv") // by default it is parquet without .format()
//    .mode(SaveMode.Overwrite)
//    .option("path", "/Users/vigneishn/Desktop/newfolder1")
//    .save()


  //  val groupedOrders = input
//    .repartition(4)
//    .where("order_customer_id > 10000")
//    .select("order_id", "order_customer_id")
//    .groupBy("order_customer_id")
//    .count()

  // input.show()
  //scala.io.StdIn.readLine()
  spark.stop()

}
