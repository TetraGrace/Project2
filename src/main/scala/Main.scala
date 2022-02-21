
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.io.StdIn._

object Main {
  def main(args: Array[String]):Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder()
      .appName("project1")
      .config("spark.master","local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val table = spark.read.json("data/yelp_academic_dataset_business.json")

    val df=table.select(col("attributes.*")) //all attributes
    val dfRest=df.filter(col("RestaurantsTakeOut") === "True" || col("RestaurantsTakeOut") === "False")
    //dfRest.show()
    //println(dfRest.count)

    // Getting table of restaurants only
    val templist = Seq("True", "False")
    val tableRest = table.filter(table("attributes")("RestaurantsTableService").isin(templist:_*));
    //tableRest.show();
    //println(tableRest.count())

    //  4. Rating to review count comparison. To gauge validity of reviews to the avg review score
    val q4 = tableRest.select("business_id", "name", "stars", "review_count");
    q4.show()

    //  7. Restaurant comparison metric
    println("Enter a restaurant's name:")
    val name = readLine();
    tableRest.createOrReplaceTempView("tableRest");

    spark.sql(s"create temp view t1 as select city, stars, review_count from tableRest where name = '$name'")

    //match across all locations
    println("Better than restaurants in all locations")
    spark.sql("select count(tableRest.name) from tableRest, t1 where tableRest.stars <= t1.stars and tableRest.review_count < t1.review_count").show()

    //match to city only
    println("Better than restaurants in same city")
    spark.sql("select t1.city, count(tableRest.name) from tableRest, t1 where t1.city = tableRest.city and tableRest.stars <= t1.stars and tableRest.review_count < t1.review_count group by t1.city").show()


    //spark.sql("select q7.business_id, SPLIT(q7.date, ',') as date_array from q7").show();
    //val q7 = temp.select(split(col("date"), ",")).as("date_split");
    //q7.show();

  }
}
