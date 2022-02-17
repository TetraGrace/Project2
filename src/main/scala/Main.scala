
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

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

    //  7. Top 10 restaurants with the most checkin’s in a day
    val checkin = spark.read.json("data/yelp_academic_dataset_checkin.json")
    //checkin.show();
    val temp = tableRest.join(checkin, tableRest("business_id") === checkin("business_id"), "inner");
    val q7 = temp.select(split(col("date"), ",")).as("date_split");
    q7.show();

  }
}
