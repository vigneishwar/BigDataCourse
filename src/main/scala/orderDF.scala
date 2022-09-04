import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object orderDF extends App {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "orders df")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val input = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("/Users/vigneishn/Downloads/orders-201019-002101.csv")

  input.show()
  scala.io.StdIn.readLine()
  spark.stop()

}
