import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object averageConnection extends App {

  // creating a named function instead of anonymous function
  def parseLine(line: String): (Int, Int) = {
    val fields = line.split("::")
    val age = fields(2).toInt
    val numConnections = fields(3).toInt
    return (age,numConnections)
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","movierating")
  val input = sc.textFile("/Users/vigneishn/Downloads/friendsdata-201008-180523.csv")
  val mappedInput = input.map(parseLine)
//  val groupMappedInput = mappedInput.map(x => (x._1,(x._2,1)))
  val groupMappedInput = mappedInput.mapValues(x => (x,1))
  val addValues = groupMappedInput.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
  val avgConnections = addValues.mapValues(x => x._1/x._2).sortBy(x => x._2)
  val results = avgConnections.collect
  results.foreach(println)

}
