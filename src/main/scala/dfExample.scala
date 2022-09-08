import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, unix_timestamp}
import org.apache.spark.sql.types.DateType


object dfExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "df example")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val myList = List((1, "2022-08-09", 1111, "CLOSED"),
    (2, "2023-08-09", 2222, "PENDING_PAYMENT"),
    (3, "2022-08-09", 1111, "COMPLETE"),
    (4, "2024-08-09", 3333, "CLOSED"))

  val df = spark.createDataFrame(myList).toDF("order_id", "order_date", "customer_id", "status")
  //  df.printSchema()
  //  df.show()

  // convert date field to a timestamp.
  // to add a new column or modify the column use .withColumn()

  val df2 = df.withColumn("order_date", unix_timestamp(col("order_date")
    .cast(DateType)))
    .withColumn("new_id", monotonically_increasing_id) // monotonically_increasing_id is a system defined id generator
    .dropDuplicates("order_date", "customer_id") // dropping duplicates
    .drop("order_id") // dropping a column
    .sort("order_date") // sorting based on a column

  df2.show()
  spark.stop()
}
