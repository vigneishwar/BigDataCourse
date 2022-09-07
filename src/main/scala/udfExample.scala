import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}


object udfExample extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "UDF in spark")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val input = spark.read
    .format("csv")
    .option("inferSchema", true)
    .option("path", "/Users/vigneishn/Downloads/-201025-223502 (1).dataset1")
    .load()

  val namedInput = input.toDF("name", "age", "city")
  namedInput.printSchema()

  spark.stop()


}
