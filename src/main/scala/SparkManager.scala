import org.apache.spark.sql._

class SparkManager {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("project2")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()


}
