import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.Source


object boringWords extends App {
  def loadBoringWords(): Set[String] = {
    var boringWord: Set[String] = Set()
    val lines = Source.fromFile("/Users/vigneishn/Downloads/boringwords-201014-183159.txt").getLines()
    for(line <- lines) {
      boringWord += line
    }
    boringWord
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","boringwords")
  val nameSet = sc.broadcast(loadBoringWords())
  val input = sc.textFile("/Users/vigneishn/Downloads/bigdatacampaigndata-201014-183159.csv")
  val mappedInput = input.map(x => (x.split(",")(10).toFloat,x.split(",")(0)))
  val mappedValues = mappedInput.flatMapValues(x => x.split(" "))
  val swapValues = mappedValues.map(x => (x._2.toLowerCase(),x._1))

  // checks if there is any boring words . If it is not a boring word then it will not be ignored.
  // Only the boring words in the set will be ignored
  val filteredWords = swapValues.filter(x => !nameSet.value(x._1))
  val finalGrouped = filteredWords.reduceByKey((x,y) => x+y)
  val sortOnValues = finalGrouped.sortBy(x => x._2,false)
  val results = sortOnValues.take(20).foreach(println) // top 20 rows

}