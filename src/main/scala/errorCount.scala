import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object errorCount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","errorcount")
  // creating a list locally and distributiing it as RDD using parallize method
  val myList = List("WARN: xxxxx", "ERROR: xxxxx", "ERROR: xxxxx", "ERROR: xxxxx", "ERROR: xxxxx", "ERROR: xxxxx")

  // use sc.defaultParallelism to check the parallelism level and
  // rdd.getNumPartitions to check the number of partitions
  val originalLogsRDD = sc.parallelize(myList)
  val mappedColumns = originalLogsRDD.map(x => {
    val columns = x.split(";")
    val cols = columns(0)
    (cols,1)
  })
  val results = mappedColumns.reduceByKey((x,y) => x+y)
  results.collect().foreach(println)

}
