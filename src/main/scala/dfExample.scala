import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession


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


  spark.stop()

}
