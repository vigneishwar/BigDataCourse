import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object sparkSqlEx extends App {

  case class Logging(level: String, datetime: String)

  def mapper(line: String): Logging = {
    val fields = line.split(',')
    val logging:Logging = Logging(fields(0), fields(1))
    return logging
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "big log text")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

//
//  import spark.implicits._
//  val myList = List("DEBUG,2015-2-6 16:24:07",
//  "WARN,2016-7-26 18:54:43",
//  "INFO,2012-10-18 14:35:19",
//  "DEBUG,2012-4-26 14:26:50",
//  "DEBUG,2013-9-28 20:27:13",
//  "DEBUG,2015-7-17 00:49:27",
//  "DEBUG,2014-7-26 02:33:09"
//  )
//
//  val rdd1 = spark.sparkContext.parallelize(myList)
//  val rdd2 = rdd1.map(mapper)
//
//  val df1 = rdd2.toDF()
//  df1.createOrReplaceTempView("logging_table")
//
//  // spark.sql("select * from logging_table").show()
//
//
////// use collect_list() to group datetime
////  spark.sql("select level, collect_list(datetime) from logging_table group by level order by level").show(false)
//
//
//  // use date_format() to extract month day date
//
//  val df2 = spark.sql("select level, date_format(datetime,'MMMM') as month from logging_table")
//
//  df2.createOrReplaceTempView("new_logging_table")
//
//  spark.sql("select level, month, count(1) from new_logging_table group by level, month").show()

  val df3 = spark.read
    .option("header", true)
    .csv("/Users/vigneishn/Downloads/bigLog.txt")

  df3.createOrReplaceTempView("my_new_log_table")

  val results = spark.sql(
    """
      |select level, date_format(datetime, 'MMMM') as month, count(1) as total
      |from my_new_log_table
      |group by level, month
      |""".stripMargin)



  spark.sql(
    """
      |select level, date_format(datetime, 'MMMM') as month
      |first(date_format(datetime, 'M')) as monthnum
      |count(1) as total
      |from my_new_log_table
      |group by level, month
      |order by month_in_nums
      |""".stripMargin).show()
  
  scala.io.StdIn.readLine()
  spark.stop()
}
