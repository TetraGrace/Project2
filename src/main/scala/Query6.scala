import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json,col}
import scala.io.StdIn.{readLine, readInt}

//codes below are just for the purpose of learning

object Query6 extends App {

  System.setProperty("hadoop.home.dir", "c:/winutils")
  val spark = SparkSession
    .builder()
    .appName("project1")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def query6(): Unit ={

      //LOAD OUR DATA IN FROM YELP
      val df = spark.read.json ("data/yelp_academic_dataset_business.json")

      //Create our list of restaurants (Yelp Data includes many more services than restaurants)
      val templist = Seq ("True", "False")
      val restaurantList = df.filter (df ("attributes") ("RestaurantsTableService").isin (templist: _*) )
      val restaurantData = restaurantList.select ("business_id", "city", "state", "categories", "stars", "review_count")

      //Create our list of possible cuisine types (filtered manually to remove things like 'Laser Tag')
      val categoriesDF = spark.read.format ("csv").option ("header", "true").load ("data/categories.txt")
      val categories = categoriesDF.select ("Categories").collect ()

      val statesList = restaurantData.select ("state").distinct ().orderBy (asc ("state") )
      statesList.show (100)
      val stateListIterator = statesList.collect ()

      //Get user input to decide what state we want to look in
      val pickedState = readLine ("Please enter one of the above state abbreviations or type exit: ")
      println (
      """
        |Do you want to order the data by Overall Rating or Total Reviews? (Enter 1 or 2)
        |1. Overall Rating
        |2. Total Reviews
        |""".stripMargin)

      val pickedInt = readInt ()
      var pickedFilter = ""
      var otherFilter =""

      if (pickedInt == 1) {
      pickedFilter = "rating"
        otherFilter = "count"
    }
      else {
      pickedFilter = "count"
        otherFilter = "rating"
    }
      //**Create our data table which includes the reviews per state**
      // WARNING: If you have not created this table yet, then it will take quite a while (10-15 mins) to create. Will work on optimization if time allows.
      //Please comment out this portion after you create the table, to allow for faster querying.
      //UPDATE: I will leave the code to see how query6.json was created. Query times are MUCH faster by loading the JSON.
//      spark.sql ("DROP TABLE IF EXISTS stateRatings")
//      spark.sql ("CREATE TABLE IF NOT EXISTS stateRatings (State String, Cuisine String, rating String, count Int)")
//      for (i <- 0 to stateListIterator.length - 1) {
//        for (j <- 0 to categories.length - 1) {
//        var testRating = restaurantData.where (s"state = '${stateListIterator (i).toString ().replaceAll ("[\\[\\]]", "")}'").filter (col ("categories").like (s"%${categories (j).toString ().replaceAll ("[\\[\\]]", "")}%") )
//        var averageRating = testRating.select (round (avg ("stars"), 2) ).as ("rating")
//        var numberReviews = testRating.select (count ("review_count").as ("Total_Review_count") )
//        val ratings = averageRating.collect ()
//        val reviews = numberReviews.collect ()
//        spark.sql (s"INSERT INTO stateRatings VALUES ('${stateListIterator (i).toString ().replaceAll ("[\\[\\]]", "")}', '${categories (j).toString ().replaceAll ("[\\[\\]]", "")}',${ratings (0).toString ().replaceAll ("[\\[\\]]", "")},${reviews (0).toString ().replaceAll ("[\\[\\]]", "")} )")
//        }
//      }

    val df3 = spark.read.json("data/query6.json")
    df3.createTempView("ratingsByState")
    spark.sql(s"SELECT * from ratingsByState WHERE rating IS NOT NULL AND state='$pickedState' ORDER BY $pickedFilter desc, $otherFilter desc").show(200)

      //Case statement to match the state chosen.
//      pickedState match {
//        case "BC" => {
//        spark.sql ("SELECT * from stateRatings").where ("rating IS NOT NULL AND state='BC'").orderBy (desc (s"$pickedFilter"), desc (s"$otherFilter") ).show (1000)
//      }
//        case "CO" => {
//        spark.sql ("SELECT * from stateRatings").where ("rating IS NOT NULL AND state='CO'").orderBy (desc (s"$pickedFilter"), desc (s"$otherFilter") ).show (1000)
//      }
//        case "FL" => {
//        spark.sql ("SELECT * from stateRatings").where ("rating IS NOT NULL AND state='FL'").orderBy (desc (s"$pickedFilter"), desc (s"$otherFilter") ).show (1000)
//      }
//        case "GA" => {
//        spark.sql ("SELECT * from stateRatings").where ("rating IS NOT NULL AND state='GA'").orderBy (desc (s"$pickedFilter"), desc (s"$otherFilter") ).show (1000)
//      }
//        case "MA" => {
//        spark.sql ("SELECT * from stateRatings").where ("rating IS NOT NULL AND state='MA'").orderBy (desc (s"$pickedFilter"), desc (s"$otherFilter") ).show (1000)
//      }
//        case "OH" => {
//        spark.sql ("SELECT * from stateRatings").where ("rating IS NOT NULL AND state='OH'").orderBy (desc (s"$pickedFilter"), desc (s"$otherFilter") ).show (1000)
//      }
//        case "OR" => {
//        spark.sql ("SELECT * from stateRatings").where ("rating IS NOT NULL AND state='OR'").orderBy (desc (s"$pickedFilter"), desc (s"$otherFilter") ).show (1000)
//      }
//        case "TX" => {
//        spark.sql ("SELECT * from stateRatings").where ("rating IS NOT NULL AND state='TX'").orderBy (desc (s"$pickedFilter"), desc (s"$otherFilter") ).show (1000)
//      }
//        case "WA" => {
//        spark.sql ("SELECT * from stateRatings").where ("rating IS NOT NULL AND state='WA'").orderBy (desc (s"$pickedFilter"), desc (s"$otherFilter") ).show (1000)
//      }
//        case _ => {
//        System.exit (0)
//      }
//    }
  }
}