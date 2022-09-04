import org.apache.spark.sql.SparkSession

object dataframeExample extends  App {

  val spark = SparkSession.builder()
    .appName("Dataframe Example")
    .master("local[2]")
    .getOrCreate()

  spark.stop()

}
