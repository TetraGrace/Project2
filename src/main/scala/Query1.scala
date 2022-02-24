import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, sum}


object Q1 extends App {

  System.setProperty("hadoop.home.dir", "c:/winutils")
  val spark = SparkSession
    .builder()
    .appName("project1")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  val df = spark.read.json("data/yelp_academic_dataset_business.json")

  def Q1: Unit = {
    val buscount = df.groupBy(col("city")).agg(count("city").as("business_count")).sort(col("business_count").desc)
    buscount.show()
    val review = df.groupBy(col("city")).agg(sum("review_count").as("reviewsum")).sort(col("reviewsum").desc)
    review.show()
    buscount.join(review, buscount("city") === review("city"), "right").show()

  }
}