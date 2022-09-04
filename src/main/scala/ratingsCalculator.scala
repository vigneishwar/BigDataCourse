import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object ratingsCalculator extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  val input = sc.textFile("/Users/vigneishn/Downloads/ratings-201019-002101 (1).dat")
  val mappedInput = input.map(x=> {
    val fields = x.split("::")
    (fields(1),fields(2))
  })
  val mappedValues = mappedInput.mapValues(x => (x.toFloat,1.0))
  val summedRatings = mappedValues.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

  val ratingFilter = summedRatings.filter(x => x._2._2 > 10)

  val avgRatings = ratingFilter.mapValues(x => x._1/x._2).filter(x => x._2 > 4.0)

  val movies_input = sc.textFile("/Users/vigneishn/Downloads/movies-201019-002101 (1).dat")

  val mappedMovies = movies_input.map(x => {
    val fields = x.split("::")
    (fields(0),fields(1))
  })

  val joinedMovies = mappedMovies.join(avgRatings)
  val topMovies = joinedMovies.map(x => x._2._1)
  topMovies.collect.foreach(println)

  scala.io.StdIn.readLine()
}
