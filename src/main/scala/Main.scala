import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql._
import scala.io.StdIn._

object Main {

  val sparkManager = new SparkManager

  def main(args: Array[String]):Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder()
      .appName("Project2")
      .config("spark.master", "local")
      .getOrCreate()


    val df = spark.read.json("data/yelp_academic_dataset_business.json")
    //df.select("business_id").where("attributes = 'RestaurantsTakeout'").show(1000)

    df.select(col("business_id"), col("name"),col("attributes.RestaurantsTableService")).where("attributes.RestaurantsTableService = true").show(100)
  }

  def Init():Unit = {
    //sparkManager.readBusinessData()
  }
}
