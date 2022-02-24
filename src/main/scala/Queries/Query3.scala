package Queries

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, split, to_timestamp, trunc}
import org.apache.spark.storage.StorageLevel

import scala.io.StdIn.readLine

object Query3 {
  def q3(spark: SparkSession, df: DataFrame): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils")
    val spark = SparkSession
      .builder()
      .appName("project1")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.json("data/yelp_academic_dataset_business.json").persist(StorageLevel.MEMORY_ONLY_SER)
    val df2 = spark.read.json("data/yelp_academic_dataset_checkin.json").persist(StorageLevel.MEMORY_ONLY_SER)

    println("Please enter the business name:")
    val business = readLine()
    val business_df = df.select(col("name"), col("review_count"), col("stars"), col("business_id")).where(col("name") === business)
    business_df.show()
    val business_id = df2.select(col("business_id")).intersect(business_df.select("business_id")).first.getString(0)
    val checkin_df = df2.select(explode(split(col("date"), ", "))).where(col("business_id") === business_id).persist(StorageLevel.MEMORY_ONLY_SER)
    checkin_df.show()
    //for zeppelin graphing
    val zeppelin = checkin_df.select(to_timestamp(col("col"))).withColumn("Month", trunc(col("to_timestamp(col)"), "month"))
    zeppelin.groupBy("month").count().show

    //checkin_df.write.csv("output")
    println(business + "total Check in times is:" + checkin_df.count())
  }
}
