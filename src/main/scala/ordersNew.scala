import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}


object ordersNew extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  case class Orders(order_id: Int, customer_id: Int, order_status: String)

  val myRegex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r
  def parser(line: String) = {
    line match {
      case myRegex(order_id, date, customer_id, order_status) =>
        Orders(order_id.toInt,customer_id.toInt,order_status )

    }
  }

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "orders unstructured")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val input = spark.sparkContext.textFile("/Users/vigneishn/Downloads/orders_new-201019-002101.csv")

  import spark.implicits._
  val structDf = input.map(parser).toDS().cache()

  structDf.printSchema()

  structDf.select("order_id").show()
  structDf.groupBy("order_status").count().show()


  scala.io.StdIn.readLine()
  spark.stop()
}
