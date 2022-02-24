import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json,col}

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

  def query2(): Unit= {

    //LOAD OUR DATA IN FROM YELP
    val df = spark.read.json("data/yelp_academic_dataset_business.json")

    //Create our list of restaurants (Yelp Data includes many more services than restaurants)
    val templist = Seq("True", "False")
    val restaurantList = df.filter(df("attributes")("RestaurantsTableService").isin(templist: _*))
    val restaurantData = restaurantList.select("business_id", "city", "categories", "stars", "review_count")

    //Create our list of possible cuisine types (filtered manually to remove things like 'Laser Tag')
    val categoriesDF = spark.read.format("csv").option("header", "true").load("data/categories.txt")
    val categories = categoriesDF.select("Categories").collect()

    //Create a table to include all of our categories
    spark.sql("DROP TABLE IF EXISTS categories")
    spark.sql("CREATE TABLE IF NOT EXISTS categories (Cuisine String)")
    for (i <- 0 to categories.length - 1) {
      spark.sql(s"INSERT INTO categories VALUES ('${categories(i).toString().replaceAll("[\\[\\]]", "")}')")
    }
    spark.sql("SELECT * FROM categories").show(1000)


    // Create our ratings table which will include our category and its average rating (stars)
    spark.sql("DROP TABLE IF EXISTS ratings")
    spark.sql("CREATE TABLE IF NOT EXISTS ratings (Cuisine String, rating String, count Int)")
    for (i <- 0 to categories.length - 1) {
      var testRating = restaurantData.filter(col("categories").like(s"%${categories(i).toString().replaceAll("[\\[\\]]", "")}%"))
      var averageRating = testRating.select(round(avg("stars"), 2)).as("rating")
      var numberReviews = testRating.select(count("review_count").as("Total_Review_count"))
      val ratings = averageRating.collect()
      val reviews = numberReviews.collect()
      spark.sql(s"INSERT INTO ratings VALUES ('${categories(i).toString().replaceAll("[\\[\\]]", "")}',${ratings(0).toString().replaceAll("[\\[\\]]", "")},${reviews(0).toString().replaceAll("[\\[\\]]", "")} )")
    }
    spark.sql("SELECT * from ratings").orderBy(desc("rating"), desc("count")).show(1000)
  }
  query2()
}