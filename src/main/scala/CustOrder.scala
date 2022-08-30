import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CustOrder extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","dollarspent")
  val input = sc.textFile("/Users/vigneishn/Downloads/customerorders-201008-180523.csv")
  val mappedInput = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))
  val summedValues = mappedInput.reduceByKey((x,y) => x+y)
  val sortedTotal = summedValues.sortBy(x => x._2)
  val results = sortedTotal.collect
  results.foreach(println)
  
}
