import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.io.StdIn._


object Query7 extends App {
    System.setProperty("hadoop.home.dir", "c:/winutils")
    val spark = SparkSession
      .builder()
      .appName("project1")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    val table = spark.read.json("data/yelp_academic_dataset_business.json")

    def q7(spark:SparkSession):Unit = {
        val table = spark.read.json("data/yelp_academic_dataset_business.json")

        val df=table.select(col("attributes.*")) //all attributes
        val dfRest=df.filter(col("RestaurantsTakeOut") === "True" || col("RestaurantsTakeOut") === "False")

        // Getting table of restaurants only
        val templist = Seq("True", "False")
        val tableRest = table.filter(table("attributes")("RestaurantsTableService").isin(templist:_*));
        tableRest.createOrReplaceTempView("tableRest");
        val total = tableRest.count()

        //  7. Restaurant comparison metric
        println("Enter a restaurant's name:")
        val name = readLine();

        spark.sql(s"create temp view t1 as select name, city, stars, review_count from tableRest where name = '$name'")

        //match across all locations
        println(s"Out of all $total restaurants, $name is better than:")
        val q7 = spark.sql("select t1.name, t1.city, count(tableRest.name) as better_than from tableRest, t1 where t1.stars >= tableRest.stars and t1.review_count > tableRest.review_count group by t1.name, t1.city")
        val q7t1 = q7.withColumn("total", lit(total));
        val all_locations = q7t1.withColumn("percentile", col("better_than") / col("total") * 100);
        //all_locations.show();

        //match to city only
        println(s"Out of all restaurants in respective cities, $name is better than:")
        val q7t3 = spark.sql("select t1.name, t1.city, count(tableRest.name) as better_than from tableRest, t1 where t1.city = tableRest.city and tableRest.stars <= t1.stars and tableRest.review_count < t1.review_count group by t1.name, t1.city")
        val t2 = spark.sql("select tableRest.city as city, count(tableRest.city) as city_count from tableRest join t1 on tableRest.city = t1.city group by tableRest.city")
        val q7t4 = q7t3.join(t2, "city")
        val city_locations = q7t4.withColumn("percentile", col("better_than") / col("city_count") * 100)
        //city_locations.show();
    }
}
