import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object dataframeExample extends  App {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "dataframe example")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
//    .appName("Dataframe Example")
//    .master("local[2]")
    .getOrCreate() // spark session is singelton i.e one spark session for one application

  spark.stop()

}
