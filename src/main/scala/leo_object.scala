import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{MapType, StringType}

import scala.io.StdIn.readLine


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
  def Q1:Unit={
    val buscount=df.groupBy(col("city")).agg(count("city").as("business_count")).sort(col("business_count").desc)
  buscount.show()
  val review=df.groupBy(col("city")).agg(sum("review_count").as("reviewsum")).sort(col("reviewsum").desc)
  review.show()
  buscount.join(review,buscount("city") === review("city") ,"right").show()
  }
  //Q2.
  df.select(col("stars"), explode(split(col("categories"), ","))).groupBy("col").avg("stars").sort(col("avg(stars)").desc).show()
  //Q3.
 def Q3:Unit= {
    val df = spark.read.json("data/yelp_academic_dataset_business.json").persist(StorageLevel.MEMORY_ONLY_SER)
    val df2= spark.read.json("data/yelp_academic_dataset_checkin.json").persist(StorageLevel.MEMORY_ONLY_SER)

    df.show()
    df2.show()

    println("Please enter the business name:")
    val business = readLine()
    val business_df=df.select(col("name"),col("review_count"),col("stars"),col("business_id")).where(col("name")=== business)
    business_df.show()
    val business_id = df2.select(col("business_id")).intersect(business_df.select("business_id")).first.getString(0)
    val checkin_df=df2.select(explode(split(col("date"),", "))).where(col("business_id")===business_id).persist(StorageLevel.MEMORY_ONLY_SER)
    checkin_df.show()
    //for zeppelin graphing
    val zeppelin=checkin_df.select(to_timestamp(col("col"))).withColumn("Month",trunc(col("to_timestamp(col)"), "month"))
    zeppelin.groupBy("month").count().show
    //split date and time, not necessary:
    //checkin_df.withColumn("date",split(col("col")," ").getItem(0)).withColumn("time",split(col("col")," ").getItem(1)).drop("col").show()

    //checkin_df.write.csv("output")
    println(business + "total Check in times is:" + checkin_df.count())
  }
}
