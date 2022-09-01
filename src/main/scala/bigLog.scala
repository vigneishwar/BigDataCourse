import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object bigLog extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  val input = sc.textFile("/Users/vigneishn/Downloads/bigLog.txt")
  val splitMapped = input.map(x => {
    val fields = x.split(":")
    (fields(0),fields(1))
    // (fields(0),1)
  })
  // using groupByKey
  val usingGroupByKey = splitMapped.groupByKey.collect().foreach(x => println(x._1, x._2.size))
  // val usingGroupByKey = splitMapped.reduceByKey((x,y)=>x+y).collect().foreach(println)
  scala.io.StdIn.readLine()

}
