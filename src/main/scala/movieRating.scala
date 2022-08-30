import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
object movieRating extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","movierating")
  val input = sc.textFile("/Users/vigneishn/Downloads/moviedata-201008-180523.csv")
  val mappedInput = input.map(x => x.split("\t")(2))

  // Use countByValue() only when there is no more operation pending. Since countByValue()
  // is an action the final result is on local and it is not an RDD
  // therefore no parellism is achieved if we perform more operations after using countByValue()

  val results = mappedInput.countByValue() // countByValue() is an action just like collect
  results.foreach(println)
//  val groupedRatings = mappedInput.map(x => (x,1))
//  val countedRatings = groupedRatings.reduceByKey((x,y) => x+y)
//  val results = countedRatings.collect
//  results.foreach(println)

}
