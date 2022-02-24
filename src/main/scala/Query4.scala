import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Query4 extends App {
    System.setProperty("hadoop.home.dir", "c:/winutils")
    val spark = SparkSession
      .builder()
      .appName("project1")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    val table = spark.read.json("data/yelp_academic_dataset_business.json")

    def q4(spark:SparkSession):Unit = {
        val df = table.select(col("attributes.*")) //all attributes
        val dfRest = df.filter(col("RestaurantsTakeOut") === "True" || col("RestaurantsTakeOut") === "False")

        // Getting table of restaurants only
        val templist = Seq("True", "False")
        val tableRest = table.filter(table("attributes")("RestaurantsTableService").isin(templist: _*));

        //  4. Rating to review count comparison. To gauge validity of reviews to the avg review score
        val q4 = tableRest.select("business_id", "name", "stars", "review_count");
    }
}
