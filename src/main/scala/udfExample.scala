import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}


object udfExample extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def ageCheck(age: Int) = {
    if (age>18) "Y" else "N"
  }

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "UDF in spark")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val input = spark.read
    .format("csv")
    .option("inferSchema", true)
    .option("path", "/Users/vigneishn/Downloads/dataset1.csv")
    .load()

  val namedInput = input.toDF("name", "age", "city")

  val parseAgeFunc = udf(ageCheck(_:Int): String)
  val adultCol = namedInput.withColumn("adult", parseAgeFunc(col("age"))) // use .withColumn to add a new column
  adultCol.printSchema()
  adultCol.show()

  spark.stop()
}
