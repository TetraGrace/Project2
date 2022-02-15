import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json,col}
import org.apache.spark.sql.types.{MapType, StringType}

//codes below are just for the purpose of learning

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
  val df2=spark.read.json("data/yelp_academic_dataset_checkin.json")
  df2.show()
  println(df2.count)
  df.printSchema()
  val df3=df.select(col("attributes.*"))
  df3.show()
  println(df3.count)
  val df4=df3.filter(col("RestaurantsTakeOut") === "True")
  df4.show()
  println(df4.count)
  //df.withColumn("attributes",from_json(col("attributes"),MapType(StringType,StringType))).show()

}