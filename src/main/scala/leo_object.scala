import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{MapType, StringType}


object project2 extends App {

  System.setProperty("hadoop.home.dir", "c:/winutils")
  val spark = SparkSession
    .builder()
    .appName("project1")
    .config("spark.master","local")
    .enableHiveSupport()
    .getOrCreate()

  val df=spark.read.json("data/yelp_academic_dataset_business.json")
  df.show()
  println(df.count)
  //val df2=spark.read.json("data/yelp_academic_dataset_checkin.json")
  //df2.show()
  //df2.printSchema()
 // println(df2.count)
  df.printSchema()
  val df3=df.select(col("attributes.*"))
  df3.show()
  println(df3.count)
  val df4=df3.filter(col("RestaurantsTakeOut") === "True")
  df4.show()
  println(df4.count)
  //df.withColumn("attributes",from_json(col("attributes"),MapType(StringType,StringType))).show()

  //Q1. grouping by city, Sorting by number of business and total review, to find the most popular city
  val buscount=df.groupBy(col("city")).agg(count("city").as("business_count")).sort(col("business_count").desc)
  buscount.show()
  val review=df.groupBy(col("city")).agg(sum("review_count").as("reviewsum")).sort(col("reviewsum").desc)
  review.show()
  buscount.join(review,buscount("city") === review("city") ,"right").show()
  //Q2.
  df.select(col("stars"), explode(split(col("categories"), ","))).groupBy("col").avg("stars").sort(col("avg(stars)").desc).show()
  //Q3.

}