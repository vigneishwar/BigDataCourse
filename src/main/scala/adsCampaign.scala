import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object adsCampaign extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","adsCampaign")
  val input = sc.textFile("/Users/vigneishn/Downloads/bigdatacampaigndata-201014-183159.csv")
  val mappedInput = input.map(x => (x.split(",")(10).toFloat,x.split(",")(0)))
  val mappedValues = mappedInput.flatMapValues(x => x.split(" "))
  val swapValues = mappedValues.map(x => (x._2.toLowerCase(),x._1))
  val finalGrouped = swapValues.reduceByKey((x,y) => x+y)
  val sortOnValues = finalGrouped.sortBy(x => x._2,false)
  val results = sortOnValues.take(20).foreach(println) // top 20 rows

}
