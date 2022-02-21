
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, col}

class QueryManager {

  def readBusinessData(sparkSession: SparkSession): Unit = {

    val df = sparkSession.read.json("data/yelp_academic_dataset_business.json")
    val df2 = df.select("*").where("attributes.RestaurantsTableService = true")

    //<editor-fold desc="Comments for below">

    //    Best restaurants & cities by avg review score and review counts
    //    Like a top 10 restaurants listing
    //    put >= 4.5 to filter best restaurants

    //</editor-fold>

    val dfQ5 = df2.groupBy("name","city", "review_count")
      .agg(avg("stars").as("review_score")).sort(col("city"))
  .sort(col("review_score").desc).sort(col("review_count").desc).where(col("review_score") >= 4.5) limit 10
    dfQ5.show(10)
  }

}
