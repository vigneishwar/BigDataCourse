import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  val input = sc.textFile("/Users/vigneishn/Desktop/search_data.rtf")
  val words = input.flatMap(x => x.split(" "))
  val wordsLowerCase = words.map(x => x.toLowerCase())
  val mappedWords = wordsLowerCase.map(x => (x,1))
  val countedWords = mappedWords.reduceByKey((a,b) => a+b)
  countedWords.collect.foreach(println)
  // scala.io.StdIn.readLine()
}
