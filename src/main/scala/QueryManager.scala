import org.apache.spark.sql._

class QueryManager {

  def readBusinessData(sparkSession: SparkSession): Unit = {
    val df = sparkSession.read.json("data/yelp_academic_dataset_business.json")
    df.show()
  }
}
