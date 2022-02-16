import org.apache.spark.sql._

class SparkManager {

  val queryManager = new QueryManager

  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession.builder()
    .appName("project2")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  def readBusinessData(): Unit = {
    queryManager.readBusinessData(spark)
  }
}
