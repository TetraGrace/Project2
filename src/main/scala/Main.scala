
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.io.StdIn._

object Main {

  //query 4
  def q4(spark:SparkSession):Unit = {
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
    q4.show();
  }


  //query 7
  def q7(spark:SparkSession):Unit = {
    val table = spark.read.json("data/yelp_academic_dataset_business.json")

    val df=table.select(col("attributes.*")) //all attributes
    val dfRest=df.filter(col("RestaurantsTakeOut") === "True" || col("RestaurantsTakeOut") === "False")

    // Getting table of restaurants only
    val templist = Seq("True", "False")
    val tableRest = table.filter(table("attributes")("RestaurantsTableService").isin(templist:_*));
    tableRest.createOrReplaceTempView("tableRest");
    val total = tableRest.count()
    //tableRest.show()

    //  7. Restaurant comparison metric
    println("Enter a restaurant's name:")
    val name = readLine();


    spark.sql(s"create temp view t1 as select name, city, stars, review_count from tableRest where name = '$name'")

    //match across all locations
    println(s"Out of all $total restaurants, $name is better than:")
    val q7p1 = spark.sql("select t1.name, t1.city, count(tableRest.name) as ranking from tableRest, t1 where t1.stars >= tableRest.stars and t1.review_count > tableRest.review_count group by t1.name, t1.city")
    q7p1.show();

    //match to city only
    println(s"Out of all restaurants in respective cities, $name is better than:")
    spark.sql("select t1.name t1.city, count(tableRest.name) as ranking from tableRest, t1 where t1.city = tableRest.city and tableRest.stars <= t1.stars and tableRest.review_count < t1.review_count group by t1.name, t1.city").show()
  }


  def main(args: Array[String]):Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    
    val spark = SparkSession
      .builder()
      .appName("project1")
      .config("spark.master","local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    //q4(spark);
    q7(spark);
    }
}
