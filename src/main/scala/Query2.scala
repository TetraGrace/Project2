import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json,col}
import org.apache.spark.sql.types.{MapType, StringType}

//codes below are just for the purpose of learning

object Query2 extends App {

  System.setProperty("hadoop.home.dir", "c:/winutils")
  val spark = SparkSession
    .builder()
    .appName("project1")
    .config("spark.master","local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val df=spark.read.json("data/yelp_academic_dataset_business.json")
  println(df.count)
  df.show()
  val df2=spark.read.json("data/yelp_academic_dataset_checkin.json")
  println(df2.count)
  df.printSchema()
  val df3=df.select(col("attributes.*"))
  println(df3.count)
//  val df4=df3.filter(col("RestaurantsTakeOut") === "True" || col("RestaurantsTakeOut") === "False" )
  val df4=df3.filter(col("RestaurantsTableService") === "True" || col("RestaurantsTableService") === "False" )
  df4.show()
  println(df4.count)

  //df.withColumn("attributes",from_json(col("attributes"),MapType(StringType,StringType))).show()
  df.select(col("stars"), explode(split(col("categories"), ","))).groupBy("col").avg("stars").sort(col("avg(stars)").desc).show(false)

  val templist = Seq("True", "False")
  val t2 = df.filter(df("attributes")("RestaurantsTableService").isin(templist:_*))
  t2.show(1000)

  //dataframe6 = dataframe1(SELECT
}