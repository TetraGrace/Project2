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
//  println(df.count)
//  df.show()
//  val df2=spark.read.json("data/yelp_academic_dataset_checkin.json")
//  println(df2.count)
//  df.printSchema()
//  val df3=df.select(col("attributes.*"))
//  println(df3.count)
////  val df4=df3.filter(col("RestaurantsTakeOut") === "True" || col("RestaurantsTakeOut") === "False" )
//  val df4=df3.filter(col("RestaurantsTableService") === "True" || col("RestaurantsTableService") === "False" )
//  df4.show()
//  println(df4.count)
//
//  //df.withColumn("attributes",from_json(col("attributes"),MapType(StringType,StringType))).show()
//  df.select(col("stars"), explode(split(col("categories"), ","))).groupBy("col").avg("stars").sort(col("avg(stars)").desc).show(false)
//
  val templist = Seq("True", "False")
  val t2 = df.filter(df("attributes")("RestaurantsTableService").isin(templist:_*))
  t2.show(1000)
  val t3 = t2.select("business_id", "city", "categories", "stars", "review_count")
  t3.show(1000)
  (println(t3.count()))
//  val t4 = t3.select(explode(split(col("categories"), ","))).dropDuplicates().orderBy(asc("categories"))
//  t4.show(1000)
//  println(t4.count())

  val categoriesDF = spark.read.format("csv").option("header","true").load("data/categories.txt")
  categoriesDF.show(100)
  val categories = categoriesDF.select("Categories").collect()
  for (i <- 0 to categories.length-1){
    println(categories(i).toString().replaceAll("[\\[\\]]",""))
  }
//  println(categories.length)

//  spark.sql("DROP TABLE IF EXISTS categories")
//  spark.sql("CREATE TABLE IF NOT EXISTS categories (Cuisine String)")
//  for (i <- 0 to categories.length-1){
//    spark.sql(s"INSERT INTO categories VALUES ('${categories(i).toString().replaceAll("[\\[\\]]","")}')")
//  }
  spark.sql("SELECT * FROM categories").show(1000)

  val testRating = t3.filter(col("categories").like("%Chinese%"))
  testRating.show()
  val averageChineseRating = testRating.select(round(avg("stars"), 2)).as("Average Stars")
  averageChineseRating.show()

//  val categories = Seq(categoriesDF)
//  println(categories(0))
//  val t10 = df.filter(df("categories").isin(categories:_*))
//  t10.show()


  //dataframe6 = dataframe1(SELECT
}